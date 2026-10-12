"""
Service Topology Knowledge Graph for Autonomous Remediation Agent

Manages service dependencies, blast radius, and infrastructure topology using Neo4j.
Based on OpenSRE's infrastructure-neo4j skill and knowledge graph architecture.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    GraphDatabase = None


class RelationshipType(str, Enum):
    CALLS = "CALLS"
    DEPENDS_ON = "DEPENDS_ON"
    DEPLOYED_ON = "DEPLOYED_ON"
    HOSTS = "HOSTS"
    CONNECTS_TO = "CONNECTS_TO"
    SHARES_DATABASE = "SHARES_DATABASE"
    MONITORED_BY = "MONITORED_BY"
    ALERTS_TO = "ALERTS_TO"
    OWNED_BY = "OWNED_BY"
    VERSION_OF = "VERSION_OF"


class NodeType(str, Enum):
    SERVICE = "Service"
    DEPLOYMENT = "Deployment"
    POD = "Pod"
    NODE = "Node"
    CLUSTER = "Cluster"
    DATABASE = "Database"
    MESSAGE_QUEUE = "MessageQueue"
    CACHE = "Cache"
    LOAD_BALANCER = "LoadBalancer"
    TEAM = "Team"
    NAMESPACE = "Namespace"
    ALERT_RULE = "AlertRule"
    DASHBOARD = "Dashboard"


@dataclass
class TopologyNode:
    id: str
    type: NodeType
    name: str
    properties: Dict[str, Any] = field(default_factory=dict)
    labels: List[str] = field(default_factory=list)
    
    def to_cypher_params(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "name": self.name,
            "properties": json.dumps(self.properties),
            "labels": self.labels,
        }


@dataclass
class TopologyRelationship:
    source_id: str
    target_id: str
    type: RelationshipType
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_cypher_params(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "type": self.type.value,
            "properties": json.dumps(self.properties),
        }


class ServiceTopologyGraph:
    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "password",
        database: str = "neo4j"
    ):
        if not NEO4J_AVAILABLE:
            raise ImportError("neo4j package required. Install with: pip install neo4j")
        
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self._ensure_schema()
    
    def _ensure_schema(self):
        with self.driver.session(database=self.database) as session:
            session.run("CREATE CONSTRAINT topology_node_id IF NOT EXISTS FOR (n:TopologyNode) REQUIRE n.id IS UNIQUE")
            session.run("CREATE INDEX topology_node_type IF NOT EXISTS FOR (n:TopologyNode) ON (n.type)")
            session.run("CREATE INDEX topology_node_name IF NOT EXISTS FOR (n:TopologyNode) ON (n.name)")
            session.run("CREATE INDEX topology_rel_type IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.type)")
    
    def close(self):
        self.driver.close()
    
    def upsert_node(self, node: TopologyNode) -> str:
        with self.driver.session(database=self.database) as session:
            session.run("""
                MERGE (n:TopologyNode {id: $id})
                SET n.type = $type,
                    n.name = $name,
                    n.properties = $properties,
                    n.labels = $labels,
                    n.updated_at = datetime()
                ON CREATE SET n.created_at = datetime()
            """, node.to_cypher_params())
        return node.id
    
    def upsert_service(
        self,
        service_id: str,
        name: str,
        namespace: str = "default",
        team: Optional[str] = None,
        properties: Dict[str, Any] = None
    ) -> str:
        props = properties or {}
        props.update({"namespace": namespace})
        if team:
            props["team"] = team
        
        node = TopologyNode(
            id=service_id,
            type=NodeType.SERVICE,
            name=name,
            properties=props,
            labels=["Service", namespace]
        )
        return self.upsert_node(node)
    
    def upsert_deployment(
        self,
        deployment_id: str,
        service_id: str,
        version: str,
        replicas: int = 0,
        properties: Dict[str, Any] = None
    ) -> str:
        props = properties or {}
        props.update({"version": version, "replicas": replicas})
        
        node = TopologyNode(
            id=deployment_id,
            type=NodeType.DEPLOYMENT,
            name=f"{service_id}-{version}",
            properties=props,
            labels=["Deployment"]
        )
        self.upsert_node(node)
        
        self.upsert_relationship(TopologyRelationship(
            source_id=deployment_id,
            target_id=service_id,
            type=RelationshipType.VERSION_OF,
            properties={"version": version}
        ))
        
        return deployment_id
    
    def upsert_pod(
        self,
        pod_id: str,
        service_id: str,
        deployment_id: str,
        node_id: str,
        namespace: str,
        properties: Dict[str, Any] = None
    ) -> str:
        props = properties or {}
        props.update({"namespace": namespace})
        
        node = TopologyNode(
            id=pod_id,
            type=NodeType.POD,
            name=pod_id,
            properties=props,
            labels=["Pod", namespace]
        )
        self.upsert_node(node)
        
        self.upsert_relationship(TopologyRelationship(
            source_id=pod_id, target_id=service_id, type=RelationshipType.VERSION_OF
        ))
        self.upsert_relationship(TopologyRelationship(
            source_id=pod_id, target_id=deployment_id, type=RelationshipType.VERSION_OF
        ))
        self.upsert_relationship(TopologyRelationship(
            source_id=pod_id, target_id=node_id, type=RelationshipType.DEPLOYED_ON
        ))
        
        return pod_id
    
    def upsert_relationship(self, rel: TopologyRelationship) -> None:
        with self.driver.session(database=self.database) as session:
            query = f"""
                MATCH (source:TopologyNode {{id: $source_id}})
                MATCH (target:TopologyNode {{id: $target_id}})
                MERGE (source)-[r:{rel.type.value}]->(target)
                SET r.properties = $properties,
                    r.updated_at = datetime()
                ON CREATE SET r.created_at = datetime()
            """
            session.run(query, rel.to_cypher_params())
    
    def add_service_dependency(
        self,
        source_service: str,
        target_service: str,
        dependency_type: str = "http",
        properties: Dict[str, Any] = None
    ) -> None:
        props = properties or {}
        props.update({"dependency_type": dependency_type, "discovered_at": datetime.utcnow().isoformat()})
        
        self.upsert_relationship(TopologyRelationship(
            source_id=source_service,
            target_id=target_service,
            type=RelationshipType.CALLS,
            properties=props
        ))
    
    def add_infrastructure_relationship(
        self,
        source_id: str,
        target_id: str,
        rel_type: RelationshipType,
        properties: Dict[str, Any] = None
    ) -> None:
        self.upsert_relationship(TopologyRelationship(
            source_id=source_id,
            target_id=target_id,
            type=rel_type,
            properties=properties or {}
        ))
    
    def get_upstream_services(self, service_id: str, max_hops: int = 3) -> List[Dict[str, Any]]:
        with self.driver.session(database=self.database) as session:
            result = session.run(f"""
                MATCH path = (upstream:TopologyNode)-[:CALLS*1..{max_hops}]->(target:TopologyNode {{id: $service_id}})
                WHERE target.type = 'Service'
                RETURN upstream.id AS service_id, upstream.name AS name, upstream.properties AS properties,
                       length(path) AS hops,
                       [r IN relationships(path) | r.properties] AS path_properties
                ORDER BY hops
            """, {"service_id": service_id})
            
            return [dict(record) for record in result]
    
    def get_downstream_services(self, service_id: str, max_hops: int = 3) -> List[Dict[str, Any]]:
        with self.driver.session(database=self.database) as session:
            result = session.run(f"""
                MATCH path = (source:TopologyNode {{id: $service_id}})-[:CALLS*1..{max_hops}]->(downstream:TopologyNode)
                WHERE source.type = 'Service' AND downstream.type = 'Service'
                RETURN downstream.id AS service_id, downstream.name AS name, downstream.properties AS properties,
                       length(path) AS hops,
                       [r IN relationships(path) | r.properties] AS path_properties
                ORDER BY hops
            """, {"service_id": service_id})
            
            return [dict(record) for record in result]
    
    def get_blast_radius(self, service_id: str, max_hops: int = 3) -> Dict[str, Any]:
        upstream = self.get_upstream_services(service_id, max_hops)
        downstream = self.get_downstream_services(service_id, max_hops)
        
        with self.driver.session(database=self.database) as session:
            shared_db_result = session.run("""
                MATCH (s:TopologyNode {id: $service_id})-[:SHARES_DATABASE]-(db:TopologyNode {type: 'Database'})
                MATCH (other:TopologyNode)-[:SHARES_DATABASE]->(db)
                WHERE other.id <> $service_id AND other.type = 'Service'
                RETURN DISTINCT other.id AS service_id, other.name AS name, 
                       db.id AS db_id, db.name AS db_name
            """, {"service_id": service_id})
            
            shared_databases = [dict(r) for r in shared_db_result]
            
            shared_mq_result = session.run("""
                MATCH (s:TopologyNode {id: $service_id})-[:CONNECTS_TO]-(mq:TopologyNode {type: 'MessageQueue'})
                MATCH (other:TopologyNode)-[:CONNECTS_TO]->(mq)
                WHERE other.id <> $service_id AND other.type = 'Service'
                RETURN DISTINCT other.id AS service_id, other.name AS name,
                       mq.id AS mq_id, mq.name AS mq_name
            """, {"service_id": service_id})
            
            shared_queues = [dict(r) for r in shared_mq_result]
            
            deployments_result = session.run("""
                MATCH (s:TopologyNode {id: $service_id})<-[:VERSION_OF]-(d:TopologyNode {type: 'Deployment'})
                RETURN d.id AS deployment_id, d.name AS name, d.properties AS properties
            """, {"service_id": service_id})
            
            deployments = [dict(r) for r in deployments_result]
        
        return {
            "service_id": service_id,
            "upstream": upstream,
            "downstream": downstream,
            "shared_databases": shared_databases,
            "shared_message_queues": shared_queues,
            "deployments": deployments,
            "total_affected_services": len(set([s["service_id"] for s in upstream + downstream])),
            "max_hops": max_hops,
        }
    
    def get_deployment_impact(self, deployment_id: str) -> Dict[str, Any]:
        with self.driver.session(database=self.database) as session:
            service_result = session.run("""
                MATCH (d:TopologyNode {id: $deployment_id})-[:VERSION_OF]->(s:TopologyNode {type: 'Service'})
                RETURN s.id AS service_id, s.name AS name
            """, {"deployment_id": deployment_id})
            
            service = service_result.single()
            if not service:
                return {"error": "Deployment not linked to service"}
            
            blast_radius = self.get_blast_radius(service["service_id"])
            blast_radius["deployment_id"] = deployment_id
            
            pods_result = session.run("""
                MATCH (p:TopologyNode {type: 'Pod'})-[:VERSION_OF]->(d:TopologyNode {id: $deployment_id})
                RETURN p.id AS pod_id, p.name AS name, p.properties AS properties
            """, {"deployment_id": deployment_id})
            
            blast_radius["pods"] = [dict(r) for r in pods_result]
            
            return blast_radius
    
    def get_service_ownership(self, service_id: str) -> Optional[Dict[str, Any]]:
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (s:TopologyNode {id: $service_id})<-[:OWNS]-(t:TopologyNode {type: 'Team'})
                RETURN t.id AS team_id, t.name AS team_name, t.properties AS properties
            """, {"service_id": service_id})
            
            record = result.single()
            if not record:
                return None
            
            return dict(record)
    
    def get_oncall_for_service(self, service_id: str) -> Optional[Dict[str, Any]]:
        ownership = self.get_service_ownership(service_id)
        if not ownership:
            return None
        
        team_id = ownership["team_id"]
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (t:TopologyNode {id: $team_id})-[:HAS_ONCALL]->(o:TopologyNode {type: 'OnCall'})
                RETURN o.id AS oncall_id, o.name AS name, o.properties AS properties
                ORDER BY o.properties.schedule_start DESC
                LIMIT 1
            """, {"team_id": team_id})
            
            record = result.single()
            if not record:
                return None
            
            return dict(record)
    
    def sync_from_kubernetes(self, k8s_client) -> Dict[str, int]:
        counts = {"services": 0, "deployments": 0, "pods": 0, "nodes": 0}
        return counts
    
    def sync_from_service_mesh(self, mesh_client) -> Dict[str, int]:
        counts = {"dependencies": 0}
        return counts
    
    def discover_from_traces(self, trace_client, lookback_hours: int = 24) -> Dict[str, int]:
        counts = {"dependencies": 0}
        return counts
    
    def get_all_services(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.driver.session(database=self.database) as session:
            if namespace:
                result = session.run("""
                    MATCH (s:TopologyNode {type: 'Service'})
                    WHERE s.properties CONTAINS $namespace
                    RETURN s.id AS id, s.name AS name, s.properties AS properties
                """, {"namespace": f'"namespace": "{namespace}"'})
            else:
                result = session.run("""
                    MATCH (s:TopologyNode {type: 'Service'})
                    RETURN s.id AS id, s.name AS name, s.properties AS properties
                """)
            
            return [dict(r) for r in result]
    
    def get_critical_path(self, source_service: str, target_service: str) -> List[Dict[str, Any]]:
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH path = (source:TopologyNode {id: $source_id})-[:CALLS*]->(target:TopologyNode {id: $target_id})
                WHERE source.type = 'Service' AND target.type = 'Service'
                RETURN path, length(path) AS hops
                ORDER BY hops
                LIMIT 1
            """, {"source_id": source_service, "target_id": target_service})
            
            record = result.single()
            if not record:
                return []
            
            path = record["path"]
            nodes = []
            for node in path.nodes:
                nodes.append({
                    "id": node["id"],
                    "name": node["name"],
                    "type": node["type"],
                })
            return nodes
    
    def get_statistics(self) -> Dict[str, Any]:
        with self.driver.session(database=self.database) as session:
            stats = {}
            
            result = session.run("""
                MATCH (n:TopologyNode)
                RETURN n.type AS type, count(n) AS count
            """)
            stats["nodes_by_type"] = {r["type"]: r["count"] for r in result}
            
            result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) AS type, count(r) AS count
            """)
            stats["relationships_by_type"] = {r["type"]: r["count"] for r in result}
            
            result = session.run("MATCH (s:TopologyNode {type: 'Service'}) RETURN count(s) AS count")
            stats["total_services"] = result.single()["count"]
            
            return stats


class InMemoryTopologyGraph:
    def __init__(self):
        self.nodes: Dict[str, TopologyNode] = {}
        self.relationships: List[TopologyRelationship] = []
    
    def upsert_node(self, node: TopologyNode) -> str:
        self.nodes[node.id] = node
        return node.id
    
    def upsert_relationship(self, rel: TopologyRelationship) -> None:
        self.relationships = [r for r in self.relationships 
                             if not (r.source_id == rel.source_id and r.target_id == rel.target_id and r.type == rel.type)]
        self.relationships.append(rel)
    
    def get_upstream_services(self, service_id: str, max_hops: int = 3) -> List[Dict[str, Any]]:
        visited = set()
        queue = [(service_id, 0)]
        results = []
        
        while queue:
            current, hops = queue.pop(0)
            if current in visited or hops >= max_hops:
                continue
            visited.add(current)
            
            for rel in self.relationships:
                if rel.target_id == current and rel.type == RelationshipType.CALLS:
                    if rel.source_id not in visited:
                        source_node = self.nodes.get(rel.source_id)
                        if source_node:
                            results.append({
                                "service_id": rel.source_id,
                                "name": source_node.name,
                                "properties": source_node.properties,
                                "hops": hops + 1,
                            })
                            queue.append((rel.source_id, hops + 1))
        
        return results
    
    def get_downstream_services(self, service_id: str, max_hops: int = 3) -> List[Dict[str, Any]]:
        visited = set()
        queue = [(service_id, 0)]
        results = []
        
        while queue:
            current, hops = queue.pop(0)
            if current in visited or hops >= max_hops:
                continue
            visited.add(current)
            
            for rel in self.relationships:
                if rel.source_id == current and rel.type == RelationshipType.CALLS:
                    if rel.target_id not in visited:
                        target_node = self.nodes.get(rel.target_id)
                        if target_node:
                            results.append({
                                "service_id": rel.target_id,
                                "name": target_node.name,
                                "properties": target_node.properties,
                                "hops": hops + 1,
                            })
                            queue.append((rel.target_id, hops + 1))
        
        return results
    
    def get_blast_radius(self, service_id: str, max_hops: int = 3) -> Dict[str, Any]:
        upstream = self.get_upstream_services(service_id, max_hops)
        downstream = self.get_downstream_services(service_id, max_hops)
        
        return {
            "service_id": service_id,
            "upstream": upstream,
            "downstream": downstream,
            "shared_databases": [],
            "shared_message_queues": [],
            "deployments": [],
            "total_affected_services": len(set([s["service_id"] for s in upstream + downstream])),
            "max_hops": max_hops,
        }
    
    def get_all_services(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        services = []
        for node in self.nodes.values():
            if node.type == NodeType.SERVICE:
                if namespace is None or node.properties.get("namespace") == namespace:
                    services.append({"id": node.id, "name": node.name, "properties": node.properties})
        return services
    
    def get_statistics(self) -> Dict[str, Any]:
        from collections import Counter
        node_types = Counter(n.type.value for n in self.nodes.values())
        rel_types = Counter(r.type.value for r in self.relationships)
        return {
            "nodes_by_type": dict(node_types),
            "relationships_by_type": dict(rel_types),
            "total_services": node_types.get(NodeType.SERVICE.value, 0),
        }


def create_topology_graph(use_neo4j: bool = True, **kwargs) -> ServiceTopologyGraph:
    if use_neo4j and NEO4J_AVAILABLE:
        try:
            return ServiceTopologyGraph(**kwargs)
        except Exception as e:
            print(f"Failed to connect to Neo4j, falling back to in-memory: {e}")
            return InMemoryTopologyGraph()
    return InMemoryTopologyGraph()