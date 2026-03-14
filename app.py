from fastapi import FastAPI
from mangum import Mangum

# root_path tells FastAPI it's hosted at /Prod on AWS
app = FastAPI(title="Ghostie Middleware", version="1.0.0", root_path="/Prod")

@app.get("/api")
def my_api():
    return {"message": "Hello from the new API!"}

# Mangum wraps your FastAPI app so it works perfectly inside AWS Lambda
handler = Mangum(app)