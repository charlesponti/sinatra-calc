get('/adv-calc') { @title = 'Advanced Calculator'; erb :'pages/adv_calc' }

post '/adv-calc' do
  @result = params[:num1].to_i ** params[:num2].to_i
  erb :'pages/adv_calc'
end