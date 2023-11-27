class EmailsController < ApplicationController
  include Jets::AwsServices
  def producer
    sqs_client = Aws::SQS::Client.new
    begin
      queue_url = sqs_client.get_queue_url({queue_name: 'email'})[:queue_url]
    rescue
      render json: { data: 'Queue not exist!' }
      return
    end

    messages = params[:data]
    batch_messages = []
    
    messages.each do |message|
      uuid = UUID.new
      batch_messages.push({
        id: uuid.generate, 
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
