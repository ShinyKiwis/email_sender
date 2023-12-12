class SchedulerJob < ApplicationJob
  class_timeout 300

  rate '1 minute'
  iam_policy("sqs", "ses")
  def scheduler 
    begin
      sqs_client = Aws::SQS::Client.new 
      email_queue_url = sqs_client.get_queue_url({queue_name: 'email'})[:queue_url]
    rescue Aws::SQS::Errors::NonExistentQueue
      return
    end

    messages = sqs_client.receive_message({
      queue_url: email_queue_url,
    })[:messages]

    return if messages.length.zero?
    
    loop_time = 50
    loop_time.times do
      prev_ns = Time.now.nsec
      PollerJob.perform_now(:poller)
      delta_ns = Time.now.nsec - prev_ns
      if delta_ns < 1_000_000_000
        sleep_secs = (1_000_000_000.0 - delta_ns) / 1_000_000_000 
        sleep(sleep_secs)
      end
    end
  end
end