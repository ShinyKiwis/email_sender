require 'json'
class PollerJob < ApplicationJob
  class_timeout 300

  MAX_NUMBER_OF_MESSAGES = 10
  WAIT_TIME_SECONDS = 5
  
  MONGO_DATABASE_NAME = "mail_sender"

  @@mongo_client = Mongo::Client.new("mongodb+srv://#{ENV["MONGO_USERNAME"]}:#{ENV["MONGO_PASSWORD"]}@cluster0.dcnyfbf.mongodb.net/?retryWrites=true&w=majority")

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

    for message in messages
      parsed_message = JSON.parse(message.body, symbolize_names: true)
      user_uuid = parsed_message[:user_uuid]

      if is_email_not_sent_to_user?(user_uuid)
        send_email(parsed_message, user_uuid)
      end

      sqs_client.delete_message({
        queue_url: email_queue_url,
        receipt_handle: message.receipt_handle
      })
    end
  end

  private
  def send_email(parsed_message, user_uuid)
    ses_client = Aws::SES::Client.new
    mail_database = @@mongo_client.use(MONGO_DATABASE_NAME)
    email_messages = mail_database["email_messages"]

    to_address = parsed_message[:to_address]
    message_subject = parsed_message[:message][:subject]
    message_body = parsed_message[:message][:body]

    dynamic_message_body = create_dynamic_message_for_user(message_body, get_user_data(user_uuid))

    response = ses_client.send_email({
      destination: {
        to_addresses: [
          to_address
        ]
      },
      message: {
        body: {
          html: {
            charset: "UTF-8",
            data: dynamic_message_body 
          }
        },
        subject: {
          charset: "UTF-8",
          data: message_subject 
        }
      },
      source: ENV["EMAIL"]
    })
    document = {
      receiver_id: BSON::Binary.from_uuid(user_uuid),
      external_id: response.message_id,
      text_body: message_body,
      subject: message_subject,
      email_campaign_id: nil,
      bounced_at: nil,
      complained_at: nil,
      delivered_at: nil,
      opened_at: nil,
      clicked_at: nil,
    }
    email_messages.insert_one(document)
  end
  
  def is_email_not_sent_to_user?(user_uuid)
    mail_client = @@mongo_client.use(MONGO_DATABASE_NAME)
    users = mail_client["email_messages"]
    user = users.find("receiver_id": BSON::Binary.from_uuid(user_uuid))
    return user.first.nil?
  end

  def get_user_data(user_uuid)
    mail_client = @@mongo_client.use(MONGO_DATABASE_NAME)
    users = mail_client["users"]
    user = users.find("_id": BSON::Binary.from_uuid(user_uuid)).first
    user_data = Hash.new

    # Not include "_id"
    user.keys[1..].zip(user.values[1..]).each do |key, value|
      user_data[key] = value
    end
    return user_data
  end

  def create_dynamic_message_for_user(message, user_data)
    dynamic_message = message
    user_data.each do |key, value|
      dynamic_message.gsub!("{{#{key}}}", value)
    end
    return dynamic_message
  end
end