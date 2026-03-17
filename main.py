from fastapi import FastAPI
app = FastAPI()

@app.get("/api/all")
async def get_all():
    return {"status": "NSE Stock API LIVE!", "stocks": ["RELIANCE", "TCS"]}

@app.get("/")
async def root():
    return {"message": "Backend running!"}
