from contextlib import asynccontextmanager

from fastapi import FastAPI

import os

os.environ["APP_NAME"] = "test_saq"

from saq import Queue
from saq.queue.redis import RedisQueue
from saq.web.starlette import saq_web

# import it from your module where you defined your settings
# queue = Queue.from_url("postgres://postgres@localhost")

queue = RedisQueue.from_url("redis://10.200.16.74:6379/14", name="demo")
print(queue)




@asynccontextmanager
async def lifespan(app: FastAPI):
    await queue.connect()

    yield


app = FastAPI(lifespan=lifespan)


app.mount("/monitor", saq_web("/monitor", queues=[queue]))

# end-of-example
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
