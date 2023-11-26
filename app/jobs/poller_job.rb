class PollerJob < ApplicationJob
  class_timeout 300

  MAX_NUMBER_OF_MESSAGES = 3
  WAIT_TIME_SECONDS = 1

  iam_policy("sqs", "ses")
  def poller 
    sqs_client = Aws::SQS::Client.new
    email_queue_url = sqs_client.get_queue_url({queue_name: 'email'})[:queue_url]
    response = sqs_client.receive_message({
      queue_url: email_queue_url,
      max_number_of_messages: MAX_NUMBER_OF_MESSAGES,
      wait_time_seconds: WAIT_TIME_SECONDS
    })
    
    begin
      messages = response.messages
    rescue 
      puts "No messages in queue"
      return
    end

    ses_client = Aws::SES::Client.new
    for message in messages
      to_address = message.body
      ses_client.send_email({
        destination: {
          to_addresses: [
            to_address
          ]
        },
        message: {
          body: {
            html: {
              charset: "UTF-8",
              data: "Test message from <b>SES</b>!"
            }
          },
          subject: {
            charset: "UTF-8",
            data: "Test email from SES"
          }
        },
        source: ENV["EMAIL"]
      })
      sqs_client.delete_message({
        queue_url: email_queue_url,
        receipt_handle: message.receipt_handle
      })
    end
  end
end