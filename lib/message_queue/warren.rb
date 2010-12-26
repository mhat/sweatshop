module MessageQueue
  class Warren
    class IncompleteOperation < StandardError; end
    class NecromancyRequired  < StandardError; end


    def initialize(opts={})
      @rabbits        = []
      @enqueue_cursor = 0
      @dequeue_cursor = 0
      @confirm_cursor = nil

      if not opts['warren'] and not opts['warren'].is_a?(Array)
        raise ArgumentError, "Your configuration is missing a warren of rabbits!"
      else
        opts['warren'].each do |rabbit_opts|
          @rabbits << FuzzyRabbit.new(rabbit_opts)
        end
      end
    end


    def queue_size(queue_name)
      queue_size = 0

      self.each do |rabbit|
        begin
          queue_size += rabbit.queue_size(queue_name)
        rescue MessageQueue::FuzzyRabbit::MarkedDead 
        end
      end

      return queue_size
    end

 
    def queue_sizes(queue_name)
      queue_sizes = {}

      self.each do |rabbit| 
        begin
          queue_sizes[rabbit.to_key] = rabbit.queue_size(queue_name)
        rescue MessageQueue::FuzzyRabbit::MarkedDead 
          queue_sizes[rabbit.to_key] = nil
        end
      end

      return queue_sizes
    end


    def delete
      self.each do |rabbit|
        rabbit.delete
      end
    end


    def stop
      self.each do |rabbit|
        rabbit.stop 
      end
    end


    def each(&block)
      errors_on = [] 
      @rabbits.each do |rabbit|
        begin
          block.call(rabbit)
        rescue MessageQueue::FuzzyRabbit::MarkedDead
          errors_on << rabbit.to_s
        end
      end

      if errors_on.any?
        msg = "operation not run on: #{ errors_on.join(',') }"
        raise IncompleteOperation, msg
      end
    end


    ## HACK: to support fanout exchanges until the Java/Scala clients catch up
    def client
      return @rabbits.first.client
    end

    def clients
      return @rabbits
    end

    def confirm(*args)
      return nil unless @confirm_cursor 
      rabbit          = @rabbits[@confirm_cursor]
      ret             = rabbit.confirm(*args)
      @confirm_cursor = nil 
      return ret
    end


    ## dequeue - will rotate and dequeue until (a) all queues have been checked
    ##           or (b) a message is dequeued
    def dequeue(*args)
      errors = 0
      ret    = nil

      @rabbits.size.times do 
        begin
          rabbit = @rabbits[move_dequeue_cursor]
          if ret = rabbit.dequeue(*args)
            @confirm_cursor = @dequeue_cursor 
            break
          end
        rescue MessageQueue::FuzzyRabbit::MarkedDead
          errors += 1
        end
      end

      if errors == @rabbits.size 
        puts "#{self.class}#dequeue: Raising NecromancyRequired -- All Rabbits Are Dead!"
        raise MessageQueue::Warren::NecromancyRequired, "All Rabbits are dead"
      end

      return ret 
    end


    def enqueue(*args)
      each_until_write do |rabbit|
        rabbit.enqueue(*args)
      end
    end


    def publish_to_fanout(*args)
      each_until_write do |rabbit|
        rabbit.publish_to_fanout(*args)
      end
    end

    ##  enqueue - will rotate and enqueue
    def each_until_write(&block)
      errors = 0
      ret    = nil 

      @rabbits.size.times do 
        begin
          rabbit = @rabbits[move_enqueue_cursor]
          ret    = block.call(rabbit)
          break 
        rescue MessageQueue::FuzzyRabbit::MarkedDead
          errors += 1
        end
      end

      if errors == @rabbits.size 
        puts "#{self.class}#each_until_write: Raising NecromancyRequired -- All Rabbits Are Dead!"
        raise MessageQueue::Warren::NecromancyRequired, "All Rabbits are dead"
      end

      return ret
    end 



    private

    def move_enqueue_cursor
      if @rabbits.size - 1 > @enqueue_cursor
        @enqueue_cursor += 1
      else
        @enqueue_cursor  = 0
      end
      return @enqueue_cursor
    end

    def move_dequeue_cursor
      if @rabbits.size - 1 > @dequeue_cursor
        @dequeue_cursor += 1
      else
        @dequeue_cursor  = 0
      end
      return @dequeue_cursor
    end

  end



  class FuzzyRabbit < Rabbit

    class MarkedDead < RuntimeError; end

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
      @seconds_to_wait_before_down_test   =  90

      ## state
      @consecutive_errors_detected        =   0
      @marked_dead_at                     = nil

      @next_should_mark_down_test_at      =   0
      @marked_down_at                     = nil
      @marked_down_for_operations         =  []

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


    def publish_to_fanout(fanout_name, opts)
      ## carrot mutates opts, doesn't place nice with recalling this methed via warren
      dopts   = opts.clone
      durable = dopts.delete :durable || false 
      body    = dopts.delete :body

      send_command(:write) do 
        client.fanout(fanout_name, :durable => durable).publish(body, dopts) 
      end
    end
    

    ## override methods from MessageQueue::Rabbit 

    ## curious as it is -- confirm is considered a read because otherwise 
    ## dequeue wouldn't work as desired 
    def confirm(queue)
      send_command(:read) do
        task = client.queue(queue).ack
      end
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


    def delete(queue)
      send_command(:write) do
        client.queue(queue).delete
      end
    end


    def dequeue(queue)
      send_command(:read) do
        task = client.queue(queue).pop(:ack => true)
        return nil unless task
        return Marshal.load(task)
      end
    end


    def enqueue(queue, data)
      send_command(:write) do 
        client.queue(queue, :durable => true).publish(Marshal.dump(data), :persistent => true)
      end
    end


    def queue_size(queue)
      send_command(:read) do
        client.queue(queue).message_count
      end
    end


    def send_command(optype, &block)
      begin

        if marked_dead? 
          if marked_dead_and_ready_to_be_resuscitated?
            mark_alive
          else
            resuscitation_in_seconds = @seconds_to_wait_before_retry - (Time.now.to_i - @marked_dead_at)
            raise MarkedDead, "Dead and not ready for resuscitation (#{resuscitation_in_seconds})"
          end
        end

        if not marked_down?(optype)
          if should_mark_as_down?(optype)
            mark_down
            ## puts "#{self.class}#send_command: #{self.to_key} marked_down for #{optype} by administrative request"
            raise MarkedDead, "Marked down by administrative request"
          end
        else
          if marked_down_and_ready_to_be_resuscitated?
            mark_alive
          else 
            ## whine a lot, but not too much
            ## if Time.now.to_i % 2 == 0
            ##   puts "#{self.class}#send_command: #{self.to_key} still down for #{optype} by administrative request"
            ## end
            raise MarkedDead, "Still down by administrative request"
          end 
        end 

        ## it may be worth wrapping this is a SystemTimer ala RedisProxy
        ret = block.call
        @consecutive_errors_detected = 0
        return ret

      rescue Errno::ECONNREFUSED,
             Errno::ECONNRESET,
             Errno::EPIPE,
             Errno::ECONNABORTED,
             Errno::EBADF,
             Errno::EAGAIN,
             Carrot::AMQP::Server::ServerDown => ex

        @consecutive_errors_detected += 1
        @client = nil

        if not should_mark_as_dead?
          retry
        else
          puts "#{self.class}#send_command: #{self.to_key} marked_dead -> #{ex}"
          mark_dead
          raise MarkedDead, ex
        end
      end
    end


    def stop
      begin 
        SystemTimer.timeout_after(@timeout_after_seconds) do 
          @client.stop if @client
        end
      rescue
      end
    ensure
      @client = nil
    end

    ## death related book-keeping 

    def mark_alive
      @marked_down_at              = nil
      @marked_dead_at              = nil
      @consecutive_errors_detected = 0
    end

    def mark_dead
      @marked_dead_at = Time.now.to_i
    end

    def marked_dead?
      !! @marked_dead_at
    end

    def admin_mark_down!(optypes=[:read, :write])
      CACHE.set("#{self.class.to_s}:v1:#{to_key}:disable", optypes)
    end

    def admin_mark_alive!
      CACHE.delete("#{self.class.to_s}:v1:#{to_key}:disable")
    end

    def mark_down
      @marked_down_at = Time.now.to_i
    end

    def marked_down?(optype=nil)
      (!! @marked_down_at) and (optype ? @marked_down_for_operations.include?(optype) : true)
    end

    def should_mark_as_down?(optype=nil)
      if Time.now.to_i > @next_should_mark_down_test_at
        ## schedule the next test 
        @next_should_mark_down_test_at = Time.now.to_i + @seconds_to_wait_before_down_test

        if optypes = CACHE.get("#{self.class.to_s}:v1:#{to_key}:disable")
          @marked_down_for_operations = optypes
          if optype == nil || optype && @marked_down_for_operations.include?(optype)
            return true 
          else 
            return false 
          end
        else
          return false
        end
      end
      return marked_down?(optype)
    end

    def marked_down_and_ready_to_be_resuscitated?
      (marked_down?) and (not should_mark_as_down?)
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

    def to_key
      "#{@host}:#{@port}"
    end
  end

end
