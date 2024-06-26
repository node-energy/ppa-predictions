from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from fastapi import FastAPI, Request, status
from starlette.responses import Response


class ApiKeyAuthMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: FastAPI,
        api_key: str
    ):
        super().__init__(app)
        self.api_key = api_key

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint
    ) -> Response:
        api_key_header = request.headers.get("X-Api-Key")

        if not api_key_header or api_key_header != self.api_key:
            return Response(
                status_code=status.HTTP_401_UNAUTHORIZED
            )

        response = await call_next(request)
        return response
