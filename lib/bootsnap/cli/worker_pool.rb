# frozen_string_literal: true

module Bootsnap
  class CLI
    class WorkerPool
      class << self
        def create(size:, jobs:)
          if size > 0 && Process.respond_to?(:fork)
            # Inline.new(jobs: jobs)
            ForkExecutor.new(size: size, jobs: jobs)
            # new(size: size, jobs: jobs)
          else
            Inline.new(jobs: jobs)
          end
        end
      end

      class Inline
        def initialize(jobs: [])
          @jobs = jobs
        end

        def call
          @jobs.each(&:call)
          nil
        end
      end

      class ThreadExecutor
        def initialize(size:, jobs: [])
          @size = size
          @queue = ::Queue.new.tap do |q|
            jobs.each do |job|
              q.push(job)
            end
          end
          freeze
        end

        def call
          @size.times.map do
            Thread.new do
              loop do
                @queue.pop(true).call
              end
            rescue ::ThreadError
              puts 'completed'
            end
          end.each(&:join)
        end
      end

      class ForkExecutor
        def initialize(size:, jobs: [])
          @size = size
          @jobs = jobs.to_a
          @pids = []
          @queue = ::Queue.new.tap do |q|
            @jobs.each_with_index do |_job, index|
              q.push(index)
            end
          end
        end

        def call
          puts 'rr1'
          @workers = @size.times.map do
            Worker.new do |job_index|
              @jobs[job_index]
            end
          end.tap do |workers|
            workers.each(&:spawn)
          end
          @dispatcher_thread = Thread.new { dispatch_loop }
          @dispatcher_thread.abort_on_exception = true
          @queue.close
          @dispatcher_thread.join
          @workers.each do |worker|
            _pid, status = Process.wait2(worker.pid)
            return status.exitstatus unless status.success?
          end
        end

        private

        def dispatch_loop
          puts 'rr2'
          loop do
            job = @queue.pop
            return true unless job
            begin
              @workers.sample.write(job, block: false)
            rescue ::IO::WaitWritable
              begin
                free_worker.write(job, block: false)
              rescue ::IO::WaitWritable
                retry
              end
            end
          rescue ::StandardError => e
            $stderr.puts(['dbg', e.message, e.inspect])
            raise "hohohohoh"
          end
        ensure
          @workers.each do |worker|
            worker.write([:exit])
            worker.close
          end
        end

        def free_worker
          @workers.map do |worker|
            [worker.to_io, worker]
          end.to_h.then do |mapping|
            mapping[::IO.select(nil, mapping.keys)[1].sample(random: ::SecureRandom)]
          end
        end

        class Worker
          attr_reader :to_io, :pid

          def initialize(&fetch_job)
            @fetch_job = fetch_job
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
              to_io.write_nonblock(payload)
            end
          end

          def close
            to_io.close
          end

          def work_loop
            loop do
              job_index, = Marshal.load(@pipe_out)
              return if job_index == :exit
              @fetch_job.call(job_index).call
            end
          rescue IOError
            nil
          end

          def spawn
            @pid = Process.fork do
              to_io.close
              work_loop
              exit!(0)
            end
            @pipe_out.close
            true
          end
        end
      end
    end
  end
end
