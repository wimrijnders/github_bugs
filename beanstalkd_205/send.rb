#!/usr/bin/env ruby
require_relative 'common'

def num_ready
	begin
		stats = @beanstalk.tubes[TUBE_NAME].stats
		#puts stats.inspect

		num_ready = stats['current_jobs_ready'] 	
		num_total = stats['total_jobs'] 	
		puts "num_ready: #{num_ready}, total: #{num_total}"

		num_ready
	rescue
		# NotFoundError if tube not used yet - no stats
		0
	end
end

def delete_all
	delete_count = 0

	puts "deleting all...."
	begin
		loop do
			tmp = @tube.peek(:ready)
			break if tmp.nil?

	 		job = @tube.reserve 1
			if job.nil?
				puts "Couldn't reserve job!"
				next
			end

			# delete every other job
			if delete_count %2 == 0
		 		job.delete
			else
				job.release :delay => 1
			end

			puts "deleted #{delete_count} jobs" if delete_count % 1000 == 0
			delete_count += 1
		end
	rescue
		# Usually a TimedOutError, ignore and break loop
	end

	puts "deleted #{delete_count} jobs"
end

#
# Main
#

@loop_count = 0

loop do
	num = num_ready
	if num < NUM_SENT - 100
		# Send jobs in bursts
		send_jobs(NUM_SENT - num)

		@loop_count += 1
		# Every once in a while, remove all jobs in one go
		if @loop_count % 3 == 0
			#delete_all
		end
	else
		puts "sleeping..."
		sleep 2
	end

end
