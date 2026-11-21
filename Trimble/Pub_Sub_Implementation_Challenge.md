# Campus Evaluation Challenge: In-Memory Pub/Sub Broker

## Design, Implementation & Testing (AI-Assisted)

---

### 1. The Challenge

Design and implement a lightweight, **in-memory Publish-Subscribe (Pub/Sub) message broker** using **object-oriented design**.

#### What is Pub/Sub?

In a Publish-Subscribe system, **publishers** send messages to named **topics** without knowing who will consume them. **Subscribers** register interest in topics and read messages asynchronously. A **broker** sits between them and decouples producers from consumers.

For this challenge, the broker runs in a **single process**. Message bodies live **in memory** for the lifetime of the process. Multiple publishers and subscribers may interact with the broker **concurrently**. Each subscriber maintains its own read position on each topic.

**Language:** Java, C#, C++ or Python  
**Time:** 60 minutes

---



### 2. Requirements



#### 2.1 Functional

The broker must support:

- **Multiple topics**, each holding an **ordered** sequence of messages (FIFO within a topic).
- **Publishing** a string message to a topic. Every message is assigned a sequential **offset** within that topic, starting at `0`.
- **Subscription** — a consumer registers interest in a topic under a unique `consumerId`.
- **Polling** — a consumer retrieves unread messages from a topic (up to a `maxMessages` limit), in offset order. After a successful poll, that consumer's read position must advance so the same messages are not returned again.
- **Independent consumer progress** — two consumers on the same topic must not share a single read cursor. One consumer polling must not affect another's position.



#### 2.2 Design & OOP

- Apply **object-oriented principles**: separate types with clear responsibilities. A single monolithic class that owns all state is not acceptable.
- **Subscriber read progress** (offsets) must be managed through a dedicated abstraction — not ad-hoc maps embedded throughout the broker. You choose the interface and implementations.
- **Document** any non-obvious design decisions (e.g., auto-create topic vs reject unknown topic; behavior when subscribing twice to the same topic).



#### 2.3 Concurrency

- `publish` and `poll` may be invoked from **multiple threads** on the same topic at the same time.
- Your implementation must remain correct under concurrent access. Briefly document your concurrency strategy.



#### 2.4 Durability (Design Consideration)

Messages remain in memory only. However, you must **consider what happens when the broker process restarts**:

- What state is lost?
- Should subscriber read positions be recoverable? If yes, how would you model that in your design?
- You are **not** required to persist message bodies to disk. Whether and how you persist offsets is a **design choice you must justify**.



#### 2.5 Error & Edge Behavior

Your solution must define and implement consistent behavior for invalid or boundary requests. At minimum, think through:

- Polling a topic that has no messages yet
- Polling when the consumer has already read everything published so far
- Polling a topic the consumer never subscribed to
- Publishing or polling against a topic that does not exist (if applicable)

Undefined behavior is not acceptable — state your rules explicitly.

---



### 3. Broker Operations

Your broker must expose at least these capabilities (exact naming and signatures are yours to define):


| Operation | Purpose                                                  |
| --------- | -------------------------------------------------------- |
| Publish   | Send a message to a topic; return the assigned offset    |
| Subscribe | Register a consumer on a topic                           |
| Poll      | Return up to N unread messages for a consumer on a topic |


---



### 4. Deliverables

You may use AI assistants (ChatGPT, Claude, Copilot, Cursor, etc.).

#### A. Class Diagram (Mermaid)

Show your object model: types, relationships, and how message storage and consumer progress fit together.

#### B. Working Implementation

Runnable code that implements the broker and compiles/executes in your chosen language.

#### C. Unit Tests (Mandatory)

Write unit tests using a standard framework (e.g., JUnit, NUnit/xUnit, pytest). Tests must:

- Cover the **primary publish → subscribe → poll** flow
- Prove that **two consumers on the same topic read independently**
- Cover **at least two edge or failure scenarios** you identified from Section 2.5
- **All tests you submit must pass**

We do not prescribe an exact test count. Incomplete test suites that only cover the happy path will score poorly.

#### D. AI Usage Review (In-Person)

At the end of the round, walk the interviewer through your AI prompt history and code on your laptop. No separate document submission.

---



### 5. Out of Scope

- External message queues or cloud messaging services
- Network transport layers
- Container orchestration or multi-service architectures
- Production monitoring, logging frameworks, or deployment pipelines

---



### 6. Integrity

- AI tools: **permitted**
- Collaboration with other candidates or external persons: **not permitted**
- Be prepared to explain what you built and how you used AI to build it

