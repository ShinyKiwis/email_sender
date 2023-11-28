require 'json'

class PollerJob < ApplicationJob
  class_timeout 300

  MAX_NUMBER_OF_MESSAGES = 10
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
      raise if messages.length.zero?
    rescue 
      return
    end
    send_email(messages)
  end

  private
  def send_email(messages)
    ses_client = Aws::SES::Client.new
    mongo_client = Mongo::Client.new("mongodb+srv://#{ENV["MONGO_USERNAME"]}:#{ENV["MONGO_PASSWORD"]}@cluster0.dcnyfbf.mongodb.net/?retryWrites=true&w=majority")
    mail_database = mongo_client.use("mail_sender")
    sent_list = mail_database["sent_list"]

    for message in messages
      parsed_message = JSON.parse(message.body, symbolize_names: true)
      to_address = parsed_message[:to_address]
      message_subject = parsed_message[:message][:subject]
      message_body = parsed_message[:message][:body]

      if sent_list.find({to_address: to_address}).nil?
        sent_list.insert_one({to_address: to_address})
      else
        return
      end

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
              data: message_body 
            }
          },
          subject: {
            charset: "UTF-8",
            data: message_subject 
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