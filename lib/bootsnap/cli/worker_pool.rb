# frozen_string_literal: true

module Bootsnap
  class CLI
    class WorkerPool
      class << self
        def create(size:, jobs:)
          if size > 0 && Process.respond_to?(:fork)
            new(size: size, jobs: jobs)
          else
            Inline.new(jobs: jobs)
          end
        end
      end

      class Inline
        def initialize(jobs: {})
          @jobs = jobs
        end

        def push(job, *args)
          @jobs.fetch(job).call(*args)
          nil
        end

        def spawn
          # noop
        end

        def shutdown
          # noop
        end
      end

      class Worker
        attr_reader :to_io, :pid

        def initialize(jobs)
          @jobs = jobs
          @pipe_out, @to_io = IO.pipe(binmode: true)
          # Set the writer encoding to binary since IO.pipe only sets it for the reader.
          # https://github.com/rails/rails/issues/16514#issuecomment-52313290
          @to_io.set_encoding(Encoding::BINARY)

          @pid = nil
        end

        def write(message, block: true, exception: false)
          payload = Marshal.dump(message)
          if block
            to_io.write(payload)
            true
          else
            to_io.write_nonblock(payload, exception: exception) != :wait_writable
          end
        end

        def close
          to_io.close
        end

        def work_loop
          loop do
            job, *args = Marshal.load(@pipe_out)
            if job == :exit
              @pipe_out.close
              return
            end
            @jobs.fetch(job).call(*args)
          end
        rescue IOError => e
          nil
        end

        def spawn
          @pid = Process.fork do
            to_io.close
            work_loop
            exit!(true)
          end
          @pipe_out.close
          true
        end
      end

      def initialize(size:, jobs: {})
        @size = size
        @jobs = jobs
        @queue = ::Thread::Queue.new
        @pids = []
      end

      def spawn
        @workers = @size.times.map do
          Worker.new(@jobs)
        end
        @workers.each(&:spawn)
        @dispatcher_thread = Thread.new { dispatch_loop }
        @dispatcher_thread.abort_on_exception = true
        true
      end

      def dispatch_loop
        loop do
          case job = @queue.pop
          when nil
            removed = []
            @workers.each do |worker|
              worker.write([:exit], block: false, exception: true)
              worker.close
              removed << worker
            rescue ::IO::WaitWritable => e
              next
            end
            @workers.delete_if(&removed.method(:include?))
            if @workers.empty?
              return true
            else
              ::IO.select(nil, @workers)
            end
          else
            begin
              free_worker.write(job, block: false, exception: true)
            rescue ::IO::WaitWritable => e
              retry
            end
          end
        end
      end

      def free_worker
        IO.select(nil, @workers)[1].sample
      end

      def push(*args)
        @queue.push(args)
        nil
      end

      def shutdown
        @queue.close
        @dispatcher_thread.join
        @workers.each_with_index do |worker, index|
          _pid, status = Process.wait2(worker.pid)
          return status.exitstatus unless status.success?
        end
        nil
      end
    end
  end
end
