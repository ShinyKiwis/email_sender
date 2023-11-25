class EmailsController < ApplicationController
  def producer
    email_address = params[:email_address]
    sqs_client = Aws::SQS::Client.new
    begin
      queue_url = sqs_client.get_queue_url(queue_name: 'email')[:queue_url]
    rescue
      render json: {data: 'Queue not exist!'}
    end

    sqs_client.send_message({
      queue_url: queue_url,
      message_body: email_address
    })

    render json: {action: "index", data: "#{queue_url} email send to SQS queue"}
  end
end
