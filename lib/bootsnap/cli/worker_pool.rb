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

        def write(message, block: true)
          payload = Marshal.dump(message)
          if block
            to_io.write(payload)
            true
          else
            to_io.write_nonblock(payload, exception: false) != :wait_writable
          end
        end

        def close
          to_io.close
        end

        def work_loop
          puts 'bp04'
          loop do
            raw = @pipe_out.read_nonblock(102_400)
            job, *args = Marshal.load(raw)
            return if job == :exit

            @jobs.fetch(job).call(*args)
          rescue ::IO::WaitReadable
            ::IO.select([@pipe_out], nil, nil, 1)
          end
        rescue IOError
          nil
        end

        def spawn
          @pid = Process.fork do
            to_io.close
            puts 'bp05'
            work_loop
            puts 'bp06'
            exit!(0)
          end
          @pipe_out.close
          puts 'bp07'
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
        @workers = @size.times.map { Worker.new(@jobs) }
        @workers.each(&:spawn)
        @dispatcher_thread = Thread.new { dispatch_loop }
        @dispatcher_thread.abort_on_exception = true
        true
      end

      def dispatch_loop
        puts 'bp01'
        loop do
          job = @queue.pop(timeout: 1)
          if job
            unless @workers.sample.write(job, block: false)
              free_worker.write(job)
            end
          elsif !@queue.closed?
            puts 'qclosed'
            redo
          else
            puts 'wcleaning'
            @workers.each_with_index do |worker, index|
              puts "wk: #{index}, 1"
              worker.write([:exit])
              puts "wk: #{index}, 2"
              worker.close
              puts "wk: #{index}, 3"
            end
            return true
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
        puts 'th: join'
        @dispatcher_thread.join
        puts "wke: 0"
        @workers.each_with_index do |worker, index|
          puts "wke: #{index}, 1"
          _pid, status = Process.wait2(worker.pid)
          puts "wke: #{index}, 2"
          return status.exitstatus unless status.success?
        end
        nil
      end
    end
  end
end
