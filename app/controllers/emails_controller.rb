require 'uuid'

class EmailsController < ApplicationController
  def producer
    begin
      sqs_client = Aws::SQS::Client.new
      queue_url = sqs_client.get_queue_url({queue_name: 'email'})[:queue_url]
    rescue Aws::SQS::Errors::NonExistentQueue
      response = sqs_client.create_queue({
        queue_name: 'email'
      })
      queue_url = response[:queue_url]
    end

    messages = params[:messages]

    batch_messages = []
    messages.each do |message|
      batch_messages.push({
        id: UUID.generate, 
        message_body: message.to_json 
      })
    end

    sqs_client.send_message_batch({
      queue_url: queue_url,
      entries: batch_messages
    })

    render json: { data: "#{messages.length} messages sent to SQS queue" }
  end
end