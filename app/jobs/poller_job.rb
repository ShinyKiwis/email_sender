require 'json'

class PollerJob < ApplicationJob
  extend ::Honeybadger::Plugins::LambdaExtension
  class_timeout 300

  MAX_NUMBER_OF_MESSAGES = 10
  WAIT_TIME_SECONDS = 5
  
  MONGO_DATABASE_NAME = "mail_sender"

  @@mongo_client = Mongo::Client.new("mongodb+srv://#{ENV["MONGO_USERNAME"]}:#{ENV["MONGO_PASSWORD"]}@cluster0.dcnyfbf.mongodb.net/?retryWrites=true&w=majority")

  hb_wrap_handler :poller

  iam_policy("sqs", "ses")
  def poller 
    sqs_client = Aws::SQS::Client.new
    email_queue_url = sqs_client.get_queue_url({queue_name: 'email'})[:queue_url]
    response = sqs_client.receive_message({
      queue_url: email_queue_url,
      max_number_of_messages: MAX_NUMBER_OF_MESSAGES,
      wait_time_seconds: WAIT_TIME_SECONDS
    })
    
    messages = response.messages
    return if messages.length.zero?

    for message in messages
      parsed_message = JSON.parse(message.body, symbolize_names: true)
      user_id = parsed_message[:user_id]
      template_id = parsed_message[:template_id]

      begin
        if is_email_not_sent_to_user?(user_id)
          email_message = prepare_email_message(user_id, template_id)
          message_id = send_email(email_message, user_id)
          add_user_message_to_sent_list(message_id, user_id, email_message)
        end
      rescue Aws::SES::Errors::MessageRejected => e
        Honeybadger.notify(e.message)
      rescue Aws::SES::Errors::LimitExceededException => e 
        # Keep the message in the queue to retry later
        Honeybadger.notify(e.message)
        return
      end
      sqs_client.delete_message({
        queue_url: email_queue_url,
        receipt_handle: message.receipt_handle
      })
    end
  end

  private
  def send_email(email_message, user_id)
    ses_client = Aws::SES::Client.new

    response = ses_client.send_email({
      destination: {
        to_addresses: [
          email_message[:to]
        ]
      },
      message: {
        body: {
          html: {
            charset: "UTF-8",
            data: email_message[:html_body]
          }
        },
        subject: {
          charset: "UTF-8",
          data: email_message[:subject]
        }
      },
      source: ENV["EMAIL"]
    })

    return response.message_id
  end

  def prepare_email_message(user_id, template_id)
    mail_database = @@mongo_client.use(MONGO_DATABASE_NAME)
    email_templates = mail_database["campaign_mail_templates"]
    
    campaign_email_template = email_templates.find(id: template_id.to_i).first

    user_data = get_user_data(user_id)
    to_address = user_data[:email]
    message_subject = campaign_email_template["subject"]
    message_body = campaign_email_template["template"]

    dynamic_message_body = create_dynamic_message_for_user(message_body, user_data)

    return {
      to: to_address,
      subject: message_subject,
      html_body: message_body,
      text_body: ""
    }
  end

  def add_user_message_to_sent_list(message_id, user_id, email_message)
    mail_database = @@mongo_client.use(MONGO_DATABASE_NAME)
    email_messages = mail_database["email_messages"]

    document = {
      receiver_id: user_id.to_i,
      external_id: message_id,
      text_body: email_message[:html_body],
      subject: email_message[:subject],
      email_campaign_id: nil,
      bounced_at: nil,
      complained_at: nil,
      delivered_at: nil,
      opened_at: nil,
      clicked_at: nil,
    }

    email_messages.insert_one(document)
  end
  
  def is_email_not_sent_to_user?(user_id)
    mail_client = @@mongo_client.use(MONGO_DATABASE_NAME)
    users = mail_client["email_messages"]
    user = users.find("receiver_id": user_id.to_i)
    return user.first.nil?
  end

  def get_user_data(user_id)
    mail_client = @@mongo_client.use(MONGO_DATABASE_NAME)
    users = mail_client["users"]
    user = users.find("id": user_id.to_i).first
    user.delete("_id")

    return user
  end

  def create_dynamic_message_for_user(message, user_data)
    dynamic_message = message
    user_data.each do |key, value|
      dynamic_message.gsub!("{{#{key}}}", value.to_s)
    end
    return dynamic_message
  end
end