import os
from dotenv import load_dotenv

load_dotenv()

AV_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
AV_BASE = os.getenv('ALPHAVANTAGE_BASE', 'https://www.alphavantage.co/query')

UNIVERSE = ['META','AAPL']