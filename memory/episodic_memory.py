"""
Episodic Memory System for Autonomous Remediation Agent

Stores and retrieves past incident investigations using Neo4j vector similarity search.
Based on OpenSRE's episodic memory architecture.
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

try:
    from neo4j import GraphDatabase
    from neo4j_graphrag.retrievers import VectorRetriever
    from neo4j_graphrag.embeddings import OpenAIEmbeddings
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    GraphDatabase = None
    VectorRetriever = None
    OpenAIEmbeddings = None


class EpisodeStatus(str, Enum):
    """Status of an episode"""
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    FALSE_POSITIVE = "false_positive"


@dataclass
class Episode:
    """Represents a completed incident investigation episode"""
    correlation_id: str
    title: str
    summary: str
    issue_type: str
    affected_components: List[str]
    root_cause: str
    evidence: List[str]
    skills_used: List[str]
    remediation_actions: List[str]
    effectiveness_score: float  # 0-1, how effective was the remediation
    status: EpisodeStatus
    created_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_embedding_text(self) -> str:
        """Generate text for embedding - combines key fields for similarity search"""
        parts = [
            f"Issue: {self.issue_type}",
            f"Components: {', '.join(self.affected_components)}",
            f"Root cause: {self.root_cause}",
            f"Summary: {self.summary}",
        ]
        return " | ".join(parts)


class EpisodicMemory:
    """
    Episodic memory store using Neo4j with vector similarity search.
    
    Key design principles from OpenSRE:
    - Agent-driven recall (not automatic pre-injection)
    - One episode per conversation/thread (keyed by correlation_id)
    - Vector embeddings on issue text, summary, root cause
    - Strategy playbooks synthesized from repeated episodes
    """
    
    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "password",
        database: str = "neo4j",
        embedding_model: str = "text-embedding-3-small"
    ):
        if not NEO4J_AVAILABLE:
            raise ImportError("neo4j and neo4j-graphrag packages required. Install with: pip install neo4j neo4j-graphrag")
        
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.embeddings = OpenAIEmbeddings(model=embedding_model)
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create necessary indexes and constraints"""
        with self.driver.session(database=self.database) as session:
            # Unique constraint on correlation_id
            session.run("""
                CREATE CONSTRAINT episode_correlation_id IF NOT EXISTS
                FOR (e:Episode) REQUIRE e.correlation_id IS UNIQUE
            """)
            
            # Vector index for similarity search
            session.run("""
                CREATE VECTOR INDEX episode_embedding IF NOT EXISTS
                FOR (e:Episode) ON (e.embedding)
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 1536,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            
            # Index for filtering
            session.run("CREATE INDEX episode_issue_type IF NOT EXISTS FOR (e:Episode) ON (e.issue_type)")
            session.run("CREATE INDEX episode_status IF NOT EXISTS FOR (e:Episode) ON (e.status)")
            session.run("CREATE INDEX episode_created_at IF NOT EXISTS FOR (e:Episode) ON (e.created_at)")
            session.run("CREATE INDEX episode_components IF NOT EXISTS FOR (e:Episode) ON (e.affected_components)")
    
    def close(self):
        self.driver.close()
    
    def store_episode(self, episode: Episode) -> str:
        """Store an episode in Neo4j with vector embedding"""
        embedding_text = episode.to_embedding_text()
        embedding = self.embeddings.embed_query(embedding_text)
        
        with self.driver.session(database=self.database) as session:
            session.run("""
                MERGE (e:Episode {correlation_id: $correlation_id})
                SET e.title = $title,
                    e.summary = $summary,
                    e.issue_type = $issue_type,
                    e.affected_components = $affected_components,
                    e.root_cause = $root_cause,
                    e.evidence = $evidence,
                    e.skills_used = $skills_used,
                    e.remediation_actions = $remediation_actions,
                    e.effectiveness_score = $effectiveness_score,
                    e.status = $status,
                    e.created_at = $created_at,
                    e.resolved_at = $resolved_at,
                    e.metadata = $metadata,
                    e.embedding = $embedding,
                    e.embedding_text = $embedding_text
            """, {
                "correlation_id": episode.correlation_id,
                "title": episode.title,
                "summary": episode.summary,
                "issue_type": episode.issue_type,
                "affected_components": episode.affected_components,
                "root_cause": episode.root_cause,
                "evidence": episode.evidence,
                "skills_used": episode.skills_used,
                "remediation_actions": episode.remediation_actions,
                "effectiveness_score": episode.effectiveness_score,
                "status": episode.status.value,
                "created_at": episode.created_at.isoformat(),
                "resolved_at": episode.resolved_at.isoformat() if episode.resolved_at else None,
                "metadata": json.dumps(episode.metadata),
                "embedding": embedding,
                "embedding_text": embedding_text,
            })
        
        return episode.correlation_id
    
    def search_similar(
        self,
        query_text: str,
        limit: int = 5,
        min_score: float = 0.7,
        issue_type: Optional[str] = None,
        components: Optional[List[str]] = None
    ) -> List[Episode]:
        """
        Search for similar episodes using vector similarity.
        Agent-driven: called by agent when it has concrete symptoms.
        """
        query_embedding = self.embeddings.embed_query(query_text)
        
        # Build filter
        filters = []
        params = {
            "embedding": query_embedding,
            "limit": limit,
            "min_score": min_score,
        }
        
        if issue_type:
            filters.append("e.issue_type = $issue_type")
            params["issue_type"] = issue_type
        
        if components:
            # Check if any component overlaps
            filters.append("ANY(c IN e.affected_components WHERE c IN $components)")
            params["components"] = components
        
        filter_clause = " AND ".join(filters) if filters else "true"
        
        with self.driver.session(database=self.database) as session:
            result = session.run(f"""
                CALL db.index.vector.queryNodes('episode_embedding', $limit, $embedding)
                YIELD node AS e, score
                WHERE score >= $min_score AND ({filter_clause})
                RETURN e.correlation_id AS correlation_id,
                       e.title AS title,
                       e.summary AS summary,
                       e.issue_type AS issue_type,
                       e.affected_components AS affected_components,
                       e.root_cause AS root_cause,
                       e.evidence AS evidence,
                       e.skills_used AS skills_used,
                       e.remediation_actions AS remediation_actions,
                       e.effectiveness_score AS effectiveness_score,
                       e.status AS status,
                       e.created_at AS created_at,
                       e.resolved_at AS resolved_at,
                       e.metadata AS metadata,
                       score
                ORDER BY score DESC
                LIMIT $limit
            """, params)
            
            episodes = []
            for record in result:
                episode = Episode(
                    correlation_id=record["correlation_id"],
                    title=record["title"],
                    summary=record["summary"],
                    issue_type=record["issue_type"],
                    affected_components=record["affected_components"],
                    root_cause=record["root_cause"],
                    evidence=record["evidence"],
                    skills_used=record["skills_used"],
                    remediation_actions=record["remediation_actions"],
                    effectiveness_score=record["effectiveness_score"],
                    status=EpisodeStatus(record["status"]),
                    created_at=datetime.fromisoformat(record["created_at"]),
                    resolved_at=datetime.fromisoformat(record["resolved_at"]) if record["resolved_at"] else None,
                    metadata=json.loads(record["metadata"]) if record["metadata"] else {},
                )
                episodes.append(episode)
            
            return episodes
    
    def get_episode(self, correlation_id: str) -> Optional[Episode]:
        """Retrieve a specific episode by correlation_id"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (e:Episode {correlation_id: $correlation_id})
                RETURN e
            """, {"correlation_id": correlation_id})
            
            record = result.single()
            if not record:
                return None
            
            e = record["e"]
            return Episode(
                correlation_id=e["correlation_id"],
                title=e["title"],
                summary=e["summary"],
                issue_type=e["issue_type"],
                affected_components=e["affected_components"],
                root_cause=e["root_cause"],
                evidence=e["evidence"],
                skills_used=e["skills_used"],
                remediation_actions=e["remediation_actions"],
                effectiveness_score=e["effectiveness_score"],
                status=EpisodeStatus(e["status"]),
                created_at=datetime.fromisoformat(e["created_at"]),
                resolved_at=datetime.fromisoformat(e["resolved_at"]) if e["resolved_at"] else None,
                metadata=json.loads(e["metadata"]) if e["metadata"] else {},
            )
    
    def synthesize_strategy_playbook(self, issue_type: str, min_episodes: int = 3) -> Optional[str]:
        """
        Synthesize a strategy playbook from similar resolved episodes.
        Called by agent when enough similar cases exist.
        """
        episodes = self.search_similar(
            query_text=f"Issue type: {issue_type}",
            limit=20,
            min_score=0.6,
            issue_type=issue_type
        )
        
        resolved = [e for e in episodes if e.status == EpisodeStatus.RESOLVED and e.effectiveness_score > 0.7]
        
        if len(resolved) < min_episodes:
            return None
        
        # Group by root cause patterns
        playbook = f"# Strategy Playbook: {issue_type}\n\n"
        playbook += f"Based on {len(resolved)} similar resolved incidents:\n\n"
        
        for i, ep in enumerate(resolved[:5], 1):
            playbook += f"## Case {i} (Effectiveness: {ep.effectiveness_score:.0%})\n"
            playbook += f"- **Components**: {', '.join(ep.affected_components)}\n"
            playbook += f"- **Root Cause**: {ep.root_cause}\n"
            playbook += f"- **Remediation**: {'; '.join(ep.remediation_actions)}\n"
            playbook += f"- **Skills Used**: {', '.join(ep.skills_used)}\n\n"
        
        # Extract common patterns
        playbook += "## Recommended Investigation Path\n"
        playbook += "1. Check recent deployments to affected components\n"
        playbook += "2. Query error rates and latency metrics\n"
        playbook += "3. Examine pod/container logs for stack traces\n"
        playbook += "4. Verify resource limits and saturation\n"
        playbook += "5. Check upstream/downstream dependencies\n\n"
        
        playbook += "## Common Remediation Patterns\n"
        # Analyze remediation actions for patterns
        actions = []
        for ep in resolved:
            actions.extend(ep.remediation_actions)
        
        from collections import Counter
        action_counts = Counter(actions)
        for action, count in action_counts.most_common(5):
            playbook += f"- {action} (used in {count}/{len(resolved)} cases)\n"
        
        return playbook


# In-memory fallback for development/testing
class InMemoryEpisodicMemory:
    """Fallback implementation without Neo4j for local development"""
    
    def __init__(self):
        self.episodes: Dict[str, Episode] = {}
    
    def store_episode(self, episode: Episode) -> str:
        self.episodes[episode.correlation_id] = episode
        return episode.correlation_id
    
    def search_similar(
        self,
        query_text: str,
        limit: int = 5,
        min_score: float = 0.0,
        issue_type: Optional[str] = None,
        components: Optional[List[str]] = None
    ) -> List[Episode]:
        # Simple text matching fallback
        results = []
        query_lower = query_text.lower()
        
        for ep in self.episodes.values():
            if issue_type and ep.issue_type != issue_type:
                continue
            if components and not any(c in ep.affected_components for c in components):
                continue
            
            # Simple scoring based on keyword overlap
            ep_text = ep.to_embedding_text().lower()
            score = sum(1 for word in query_lower.split() if word in ep_text) / max(len(query_lower.split()), 1)
            
            if score >= min_score:
                results.append((score, ep))
        
        results.sort(key=lambda x: x[0], reverse=True)
        return [ep for _, ep in results[:limit]]
    
    def get_episode(self, correlation_id: str) -> Optional[Episode]:
        return self.episodes.get(correlation_id)
    
    def synthesize_strategy_playbook(self, issue_type: str, min_episodes: int = 3) -> Optional[str]:
        episodes = [e for e in self.episodes.values() if e.issue_type == issue_type and e.status == EpisodeStatus.RESOLVED]
        if len(episodes) < min_episodes:
            return None
        
        playbook = f"# Strategy Playbook: {issue_type} (In-Memory)\n\n"
        for i, ep in enumerate(episodes[:5], 1):
            playbook += f"## Case {i}\n- Root Cause: {ep.root_cause}\n- Remediation: {'; '.join(ep.remediation_actions)}\n\n"
        return playbook


def create_episodic_memory(use_neo4j: bool = True, **kwargs) -> EpisodicMemory:
    """Factory function to create episodic memory instance"""
    if use_neo4j and NEO4J_AVAILABLE:
        try:
            return EpisodicMemory(**kwargs)
        except Exception as e:
            print(f"Failed to connect to Neo4j, falling back to in-memory: {e}")
            return InMemoryEpisodicMemory()
    return InMemoryEpisodicMemory()