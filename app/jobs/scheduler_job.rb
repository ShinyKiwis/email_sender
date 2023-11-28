class SchedulerJob < ApplicationJob
  class_timeout 300

  rate '1 minute'
  iam_policy("sqs", "ses")
  def scheduler 
    # Check if queue exist
    begin
      sqs_client = Aws::SQS::Client.new 
      sqs_client.get_queue_url({queue_name: 'email'})
    rescue Aws::SQS::Errors::NonExistentQueue
      return
    end
    
    loop_time = 5
    loop_time.times do
      prev_ns = Time.now.nsec
      PollerJob.perform_now(:poller)
      delta_ns = Time.now.nsec - prev_ns
      if delta_ns < 1_000_000_00
        sleep_secs = (1_000_000_000.0 - delta_ns) / 1_000_000_000 
        sleep(sleep_secs)
      end
    end
  end
end