import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import (
    CoordinatorMessage,
    ElectionMessage,
    LeaderResponse,
    NodeCreate,
    NodeResponse,
    NodeUpdate,
)
import src.election as election


def _delayed_start_election():
    time.sleep(election.STARTUP_DELAY)
    election.start_election()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    threading.Thread(target=election.heartbeat_check, daemon=True).start()
    threading.Thread(target=_delayed_start_election, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}


@app.get("/api/node-id")
def get_node_id():
    return {"node_id": election.NODE_ID}


@app.get("/api/leader", response_model=LeaderResponse)
def get_leader():
    return {"leader_id": election.current_leader, "node_id": election.NODE_ID}


@app.post("/api/election")
def receive_election(msg: ElectionMessage):
    if msg.node_id >= election.NODE_ID:
        raise HTTPException(status_code=400, detail="Sender has equal or higher ID")
    election.handle_election_message(msg.node_id)
    return {"status": "ok", "node_id": election.NODE_ID}


@app.post("/api/coordinator")
def receive_coordinator(msg: CoordinatorMessage):
    election.set_leader(msg.node_id)
    return {"status": "ok", "leader_id": msg.node_id}


@app.post("/api/election/start")
def trigger_election():
    threading.Thread(target=election.start_election, daemon=True).start()
    return {"status": "election started", "node_id": election.NODE_ID}


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)
