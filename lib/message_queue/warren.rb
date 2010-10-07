module MessageQueue
  class Warren
    class NecromancyRequired < RuntimeError; end


    def initialize(opts={})
      @rabbits = []
      @cursor  =  0

      if not opts['warren'] and not opts['warren'].is_a?(Array)
        raise ArgumentError, "Your configuration is missing a warren of rabbits!"
      else
        opts['warren'].each do |rabbit_opts|
          @rabbits << FuzzyRabbit.new(rabbit_opts)
        end
      end
    end


    def method_missing(method, *args)
      begin

        case method
        when :dequeue
          ret             = this_rabbit.send(:dequeue, *args)
          @dequeue_cursor = @cursor if ret
          return ret

        when :confirm
          that_rabbit     = @rabbits[@dequeue_cursor]
          @dequeue_cursor = nil
          return that_rabbit.send(:confirm, *args)
        end

        ## on your way good sir, on your way
        return this_rabbit.send(method, *args)

      rescue Carrot::AMQP::Server::ServerDown => ex
        if not this_rabbit.should_mark_as_dead?
          this_rabbit.disconnect
        else 
          this_rabbit.mark_dead unless this_rabbit.marked_dead?
          next_rabbit
        end

        retry
      end
    end


    def queue_size(queue_name)
      queue_size = 0

      @rabbits.each do |rabbit|
        if not rabbit.marked_dead?
          begin
            queue_size += rabbit.queue_size(queue_name)
            puts "#{rabbit} : #{ queue_size }"
          rescue Carrot::AMQP::Server::ServerDown
          end
        end
      end

      return queue_size
    end


    def rabbits
      @rabbits
    end


    def this_rabbit
      return @rabbits[@cursor]
    end


    ## @rabbits is an array of rabbit instances our goal here is to find the next
    ## living rabbit instance. rabbit corpses should be skipped over, finally raise
    ## an exception if all our rabbits are dead
    ##
    def next_rabbit
      next_alive_rabbit = @rabbits.size.times do |iter|
        move_cursor
        potential_rabbit = @rabbits[@cursor]

        ## break foo in a block is like return foo is a method
        break potential_rabbit if not potential_rabbit.marked_dead?

        if potential_rabbit.marked_dead_and_ready_to_be_resuscitated?
          potential_rabbit.mark_alive
          break potential_rabbit
        end
      end

      if next_alive_rabbit.is_a?(Rabbit)
        return next_alive_rabbit
      else
        raise MessageQueue::Warren::NecromancyRequired, "All Rabbits are Dead"
      end
    end


    private

    def move_cursor
      if @rabbits.size - 1 > @cursor
        @cursor += 1
      else
        @cursor  = 0
      end
    end
  end



  class FuzzyRabbit < Rabbit
    attr_accessor :host
    attr_accessor :port
    attr_accessor :username
    attr_accessor :password
    attr_accessor :vhost
    attr_accessor :insist

    def initialize(opts={})
      ## facts
      @timeout_after_seconds              = 0.5
      @seconds_to_wait_before_retry       =  90
      @consecutive_errors_to_mark_as_dead =   1

      ## state
      @consecutive_errors_detected        =   0
      @marked_dead_at                     = nil
      @dirty                              = false

      ## up and off to rabbit
      super(opts)

      configure
    end


    def configure
      if @opts['host'] =~ /:/
        @host, @port = @opts['host'].split(':')
        @port        = @port.to_i
      else
        @host = @opts['host']
        @port = @opts['port'].to_i
      end

      @username = @opts['user'  ]
      @password = @opts['pass'  ]
      @vhost    = @opts['vhost' ]
      @insist   = @opts['insist']
    end


    def stop
      @client.stop if @client
    ensure
      @client = nil
    end
    alias_method :disconnect, :stop


    def dequeue(queue_name)
      ret   = super(queue_name)
      @dirty = true if ret
      return ret
    end


    def confirm(queue_name)
      ret   = super(queue_name)
      @dirty = false if ret
      return ret
    end


    def client
      return @client ||= Carrot.new(
        :host   => @host, 
        :port   => @port,
        :user   => @username, 
        :pass   => @password,
        :vhost  => @vhost, 
        :insist => @insist)
    end


    def send_command(&block)
      begin
        ret = block.call
        @consecutive_errors_detected  = 0
        return ret

      rescue Carrot::AMQP::Server::ServerDown => ex
        @consecutive_errors_detected += 1
        disconnect

        if not should_mark_as_dead?
          retry
        else
          mark_dead
          raise ex
        end
      end
    end


    ## death related book-keeping 

    def mark_alive
      @marked_dead_at              = nil
      @consecutive_errors_detected = 0
    end


    def mark_dead
      @marked_dead_at = Time.now.to_i
    end


    def marked_dead?
      ! @marked_dead_at.nil?
    end


    def marked_dead_and_ready_to_be_resuscitated?
      (marked_dead?) and (Time.now.to_i - @marked_dead_at > @seconds_to_wait_before_retry)
    end

 
    def should_mark_as_dead?
      @consecutive_errors_detected >= @consecutive_errors_to_mark_as_dead
    end

    def to_s
      "FuzzyRabbit(host=#{@host}:#{@port}, dead=#{marked_dead?})"
    end

  end

end
