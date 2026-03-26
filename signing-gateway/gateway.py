import asyncio
import json
import logging
import os

from aiohttp import web, ClientSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("signing-gateway")


def parse_mpc_nodes():
    """Parse MPC_NODES env var into a list of base URLs."""
    raw = os.environ.get("MPC_NODES", "")
    if not raw:
        raise SystemExit("MPC_NODES env var is required")
    nodes = []
    for addr in raw.split(","):
        addr = addr.strip()
        if addr:
            nodes.append(f"http://{addr}")
    if len(nodes) < 2:
        raise SystemExit("MPC_NODES must list at least 2 nodes")
    return nodes


async def request_partial_signature(session, node_url, payload):
    """Request a partial signature from a single MPC node."""
    try:
        async with session.post(
            f"{node_url}/sign",
            json=payload,
            timeout=10,
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error(
                    "Node %s returned %d: %s",
                    node_url, resp.status, body,
                )
                return None
            return await resp.json()
    except Exception as exc:
        logger.error("Node %s unreachable: %s", node_url, exc)
        return None


async def handle_sign(request):
    """Fan out signing request to all MPC nodes, collect partials."""
    app = request.app
    nodes = app["mpc_nodes"]
    threshold = (len(nodes) // 2) + 1

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"error": "invalid JSON body"}, status=400,
        )

    tx_id = payload.get("transaction_id", "unknown")
    logger.info(
        "Signing request for tx %s — fanning out to %d nodes",
        tx_id, len(nodes),
    )

    async with ClientSession() as session:
        tasks = [
            request_partial_signature(session, node, payload)
            for node in nodes
        ]
        results = await asyncio.gather(*tasks)

    partials = [r for r in results if r is not None]
    logger.info(
        "tx %s: received %d/%d partial signatures "
        "(threshold=%d)",
        tx_id, len(partials), len(nodes), threshold,
    )

    if len(partials) < threshold:
        return web.json_response(
            {
                "error": "insufficient partial signatures",
                "received": len(partials),
                "threshold": threshold,
            },
            status=503,
        )

    combined = {
        "transaction_id": tx_id,
        "signature": "combined-stub-signature",
        "partials_used": len(partials),
        "threshold": threshold,
    }
    logger.info("tx %s: signature assembled", tx_id)
    return web.json_response(combined)


async def handle_health(request):
    return web.json_response({"status": "ok"})


def create_app():
    app = web.Application()
    app["mpc_nodes"] = parse_mpc_nodes()
    app.router.add_post("/sign", handle_sign)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    app = create_app()
    logger.info(
        "Signing gateway starting — MPC nodes: %s",
        app["mpc_nodes"],
    )
    web.run_app(app, host="0.0.0.0", port=8000)
