require 'sinatra'
require 'sinatra/contrib/all'
require './controllers/trippr'
require './controllers/mortgagr'
require './controllers/bmi'
require './controllers/calc'
require './controllers/adv_calc'

get('/') { @title = 'Home'; erb :'static/home' }
get('/about' ) { @title = 'About'; erb :'static/about' }
not_found { @title = '404'; erb :'static/not_found' }