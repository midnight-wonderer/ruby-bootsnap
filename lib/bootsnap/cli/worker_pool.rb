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
          puts 'bp04'
          loop do
            job, *args = Marshal.load(@pipe_out)
            if job == :exit
              puts 'work loop exited'
              @pipe_out.close
              return
            end
            @jobs.fetch(job).call(*args)
          end
        rescue IOError => e
          puts 'loop io error', e.class, e.message
          nil
        end

        def spawn
          @pid = Process.fork do
            to_io.close
            puts 'bp05'
            work_loop
            @pipe_out.close
            puts 'bp06'
            exit!(true)
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
        STDOUT.sync = true
      end

      def spawn
        @workers = @size.times.map { Worker.new(@jobs) }
        @workers.each_with_index do |worker, index|
          "spawning: w#{index}"
          worker.spawn
          puts "w#{index}-pid: #{worker.pid}"
        end
        @dispatcher_thread = Thread.new { dispatch_loop }
        @dispatcher_thread.abort_on_exception = true
        true
      end

      def dispatch_loop
        puts 'start loop'
        finished_workers = []
        loop do
          job = @queue.pop
          current_workers = @workers - finished_workers
          if job
            IO.select(nil, current_workers).tap do |(_nil, available)|
              available.sample.write(job)
            end
          else
            puts 'cleaning up'
            current_workers.each_with_index do |worker, index|
              puts "worker#{index}: p1"
              worker.write([:exit])
              puts "worker#{index}: p2"
              worker.close
              puts "worker#{index}: p3"
              finished_workers << worker
            rescue IO::WaitWritable
              puts "worker#{index}-error: #{e.class} #{e.message}"
              next
            end
            current_workers.delete_if(&finished_workers.method(:include?))
            return if current_workers.empty?
            IO.select(nil, current_workers)
            puts 'continue cleaning up...'
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
        puts "join completed; waiting..."
        original_pids = @workers.map(&:pid)
        tracked_pids = original_pids.dup
        loop do
          pid, status = Process.wait2
          next unless tracked_pids.include?(pid)
          puts "pid: #{pid}, #{original_pids.find_index(pid)}"
          tracked_pids.delete(pid)
          return status.exitstatus unless status.success?
          break if tracked_pids.empty?
        end
        # @workers.each_with_index do |worker, index|
        #   puts "wke: #{index}, 1"
        #   _pid, status = Process.wait2(worker.pid)
        #   puts "wke: #{index}, 2"
        #   return status.exitstatus unless status.success?
        # end
        # nil
      end
    end
  end
end
