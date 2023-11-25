Jets.application.routes.draw do
  post '/emails', to: 'emails#producer'
end
