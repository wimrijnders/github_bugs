require 'beaneater'
require 'json'

NUM_SENT = 100000
TUBE_NAME = "test-tube"

$in_count = 0
$out_count = 0

#
# Init beanstalk
#
# Connect to pool
@beanstalk = Beaneater::Pool.new('localhost:11300')
# Enqueue jobs to tube
@tube = @beanstalk.tubes[TUBE_NAME]

#
# Methods
#

def send_jobs num
	# Send a boatload of jobs
	(0...num).each {|c|

		# Creating json string directly (as opposed to using JSON),
		# Because this works faster
		job = "{
			\"key\"     => \"foo\",
			\"count\"   => #{$out_count},
			\"payload\" => \"#{"*"*50000}\"
		}"

		@tube.put job

		$out_count += 1
		puts "wrote #{$out_count} jobs" if $out_count % (NUM_SENT/10) == 0
	}

end

def receive_jobs
	# Process jobs from tube

	tmp = nil

	loop do
		tmp = @tube.peek(:ready)
		break if tmp.nil?

	  job = @tube.reserve
		#job = tmp

	  #puts "job : #{job.body}"
	  #puts "job count is #{JSON.parse(job.body)["count"]}"

	  job.delete

		$in_count += 1
	
		puts "Read in #{$in_count} jobs" if $in_count % 1000 == 0
	end
end
