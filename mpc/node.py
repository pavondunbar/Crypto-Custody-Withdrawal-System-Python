import hashlib
import json
import logging
import os

from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("mpc-node")

NODE_ID = os.environ.get("NODE_ID", "0")


async def handle_sign(request):
    """Produce a stub partial signature for a signing request."""
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"error": "invalid JSON body"}, status=400,
        )

    tx_id = payload.get("transaction_id", "unknown")
    logger.info("Node %s: signing tx %s", NODE_ID, tx_id)

    # Stub: deterministic partial signature derived from
    # node ID + transaction payload. A real implementation
    # would use threshold-ECDSA (e.g. GG20 / CGGMP).
    material = f"{NODE_ID}:{json.dumps(payload, sort_keys=True)}"
    partial = hashlib.sha256(material.encode()).hexdigest()

    return web.json_response({
        "node_id": NODE_ID,
        "transaction_id": tx_id,
        "partial_signature": partial,
    })


async def handle_health(request):
    return web.json_response({
        "status": "ok",
        "node_id": NODE_ID,
    })


def create_app():
    app = web.Application()
    app.router.add_post("/sign", handle_sign)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    app = create_app()
    logger.info("MPC node %s starting on port 8001", NODE_ID)
    web.run_app(app, host="0.0.0.0", port=8001)
