get('/calc') { @title = 'Calculator'; erb :'pages/calc' }

post '/calc' do
  num1 = params[:num1].to_f
  num2 = params[:num2].to_f

  case params[:formula]
     when 'add' then @result = num1 + num2
     when 'subtract' then @result = num1 - num2
     when "divide" then @result = num1 / num2
     when 'multiply' then @result = num1 * num2
  end

  erb :'pages/calc'
end