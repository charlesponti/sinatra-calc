get('/mortgagr') { @title = 'Mortgagr'; erb :'pages/mortgagr' }

post '/mortgagr' do
  p = params[:principal].to_f
  r = (params[:rate].to_f / 12)/100
  t = params[:term].to_f * 12

  @result = ((p*r)*((1 + r)**t))/(((1+r)**t) - 1).to_f.round(2)
  erb :'pages/mortgagr'
end
