Yes. And I think the reason you feel lost is **not that you lack the technical ability**. The problem is that OR is still at the stage where nobody has decomposed the problem enough for you to see where you can take ownership.

Your opportunity is actually quite good: **proof of address is a self-contained AI problem that can become your entry point into the broader OR solution design.**

I would approach this as a **mini product/architecture problem**, not as “someone gave me a Doc Studio task.”

![Image](https://images.openai.com/static-rsc-4/1UGuuyCcDsk55Ew7UQ4mT0yOKWR-tzIijxxFcPO7oBWyEYdfPN0klTisvjDyaPkZZ-l8R4ew-vzI0efaSNWdCg7jMxgfaGoW2-SlT-G8Bt-BHOYkXFpOHAg6sYQbUSiOqfRGdxF-FuSSha6Mbtphd4QromSbZAwqD-QOnjv1uEOVgSafQZfECRirYFeMct2Z?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/z5d-swPVwGMJw8iWmRH3lXJLlcX-h4GHGMqYud9pcLqaw0Pka2CnSarb1bT3J-mNoClBWIm3q7TfqU1L2y6vp64Ip8jhxvrK6ZnkIMS53dKa0gIyStmErCLY5XoeyhUveb3HEd-wUkC_dwgeKEye8B-ZEZXdBW60iAbDLw2TWlrzQV7fBfyAZu9laxAmdc0e?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/PcBpHFtuX2QOZY6F76tK4HBsoAaR7cvkd4B_C7sjr6EoiD13ijzD1PmxA1l4rYUjYO1f9u2LByNCdWBrZDkoNoRy7Y3qJEwSEaatFnEAEgfJ-QvaNIAZmya5Y25b7UthnJloMsX8dE-4cpUuTKu7Jz5d8msHfpx6tVWNk9spLIF8vHidhjkGJEQzgJR2sq3D?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/NzzDxg_mgQZ5ZlCcV_d83pwv5fUYo1rL78mWYIuVOOzrtBQXJaxIxCdr0ZwJGfw4lhEc8W4_gY3eeWrSBLvbFvetYxSMjgRO54VLc9IRDKYd4atGPvQE0GpMDHv6lGABL-i32h6Aob6t_HIzsQXgiLKV2HwvL-etiKqtbKfSeX7R1B-ntBX4-jXGit36-mqP?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/_ZgYyn6g4atgBQChiBiFG80CkzRCMn_Q22m78Fs3UkEEbUYurU4AHW8IJfL9s12n8Z1T2SquxCR7z1pcBc165cAIu3H1Dml0OAbGX3pdh9UePpVRg-akUeUyjnsAHh36y8JfQxejgp8HTCjG1PFmLI2TemzU6JH_EMjyADQUrf18UrEx0I2qrLgG2TQzk_YY?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/K9pjkrNb0Pv55Czpub9eT4GDx2jX5VSlQhDwoMsA71u4LPQgp-PdNgnML-RlCkMb4m7fDI-yXbCZTpGXdZsxyJwF2usa9P-k2fXSQYSvnmPZFWcGHtXsbIXsuLi0RxNh5p1RtloYIPVaLJB3wmvyQ-vDsISrNA3tBTHXLZK96O47IUJgjG5Qd9qqrTsN_qy9?purpose=fullsize)

# 1. First, change how you think about your role

Right now you may be thinking:

> “What AI task should I pick up?”

I want you to think:

> **“What decisions does OR need to make, and what evidence can I generate to help the team make those decisions?”**

That's a much more senior/FDE way of operating.

The MD doesn't necessarily need you to immediately write LangGraph code.

He needs someone to say:

> “For proof of address, here is the end-to-end problem, here are the possible solution patterns, here is what Doc Studio gives us, here are the gaps, here are the experiments we need, and here is what I recommend we test first.”

That is **solution design**.

And once you've done that, the coding becomes much easier.

---

# 2. Decompose the Proof of Address problem

Don't call the whole thing "proof of address AI."

Break it into stages.

A customer's journey might look like:

```text
Customer
   │
   ▼
Upload document
   │
   ▼
Document ingestion
   │
   ▼
Document classification
   │
   ▼
Document quality checks
   │
   ▼
Document authenticity / tampering checks
   │
   ▼
Information extraction
   │
   ▼
Field validation
   │
   ▼
Address normalization
   │
   ▼
Customer ↔ document matching
   │
   ▼
Policy / eligibility checks
   │
   ▼
Risk / confidence assessment
   │
   ├── High confidence → Straight-through processing
   │
   ├── Medium confidence → Human review
   │
   └── Low confidence / suspicious → Reject / investigate
   │
   ▼
Prefill onboarding form
   │
   ▼
Customer confirms/corrects
   │
   ▼
Final decision
```

**This is already a solution-design artifact.**

And notice something important:

> Only part of this is "AI."

That's actually a good thing.

A senior AI engineer shouldn't force an LLM into every box.

---

# 3. Your first mission: find out what Doc Studio actually gives you

You already know:

> HSBC has an internal Doc Studio and OR will use it.

Great.

**Don't start by reinventing document extraction.**

Your first investigation should be:

### What can Doc Studio do?

Build a capability matrix.

| Capability              | Doc Studio? | Accuracy | Output | OR requirement | Gap |
| ----------------------- | ----------- | -------: | ------ | -------------- | --- |
| Document classification | ?           |        ? | ?      | Required       | ?   |
| OCR                     | ?           |        ? | ?      | Required       | ?   |
| Field extraction        | ?           |        ? | ?      | Required       | ?   |
| Document quality        | ?           |        ? | ?      | Required       | ?   |
| Tampering detection     | ?           |        ? | ?      | Maybe          | ?   |
| Address extraction      | ?           |        ? | ?      | Required       | ?   |
| Confidence score        | ?           |        ? | ?      | Required       | ?   |
| Multiple documents      | ?           |        ? | ?      | ?              | ?   |
| Handwritten text        | ?           |        ? | ?      | ?              | ?   |
| Multilingual documents  | ?           |        ? | ?      | ?              | ?   |
| Human review            | ?           |        ? | ?      | ?              | ?   |
| API integration         | ?           |        ? | ?      | Required       | ?   |
| Audit trail             | ?           |        ? | ?      | Required       | ?   |

**This is probably the single most useful document you can create right now.**

Because after doing this, you'll know where AI work actually remains.

---

# 4. Don't assume "extract address" is the problem

This is where I want you to think beyond the obvious.

Suppose the customer uploads:

> DBS bank statement

Doc Studio extracts:

```json
{
  "name": "Jane Wong",
  "address": "123 Orchard Road, Singapore 238888",
  "statement_date": "2026-08-31"
}
```

Looks easy.

But what does OR actually need to know?

Potential questions include:

### Document-level

* Is this actually a bank statement?
* Is it an accepted proof-of-address document?
* Is it expired?
* Is the image readable?
* Is the document complete?
* Is it suspiciously edited?
* Is it a screenshot?
* Is it a photocopy?
* Is it a duplicate document?

### Field-level

* What is the customer's name?
* What is the address?
* What is the document date?
* What institution issued it?
* Is the address complete?
* Is the address residential?
* Is the address in an acceptable jurisdiction?

### Cross-source

Does:

```text
Customer-provided name
```

match:

```text
Document name
```

Does:

```text
Document address
```

match:

```text
Customer-entered address
```

Does it match another document?

Does it match existing bank information?

---

# 5. Address matching is itself an interesting AI problem

This is something I'd specifically investigate.

Consider:

**Customer form**

> 123 Orchard Road #12-04 Singapore 238888

**Document**

> 123 ORCHARD RD
> UNIT 12-04
> SINGAPORE 238888

Exact string matching says:

❌ different.

A normalization pipeline could produce:

```text
123 ORCHARD ROAD
12-04
SINGAPORE
238888
```

Then you can compare structured components.

But then:

> 123 Orchard Road #12-04

vs.

> 123 Orchard Rd, Apt 12-04

Probably same.

Then:

> 123 Orchard Road

vs.

> 123 Orchard Road #12-04

Potentially same building but insufficient unit information.

Then:

> 123 Orchard Road

vs.

> 123 Orchard Boulevard

Different.

This becomes a combination of:

**normalization + deterministic rules + fuzzy matching + potentially ML/LLM reasoning.**

You don't need an LLM just because it's an AI project.

---

# 6. The "score" needs to be defined before anyone builds it

This is an important leadership opportunity.

When the MD says:

> “We should generate a score.”

Your response shouldn't immediately be:

> “Let's train a model.”

Ask:

> **“What decision is the score supposed to support?”**

Because there could be several completely different scores.

### A. Extraction confidence

> How confident are we that we extracted the address correctly?

Example:

```text
Address extraction confidence = 0.97
```

### B. Document validity score

> How likely is this to be an acceptable proof-of-address document?

### C. Identity consistency score

> How strongly does the document's information agree with the customer's information?

### D. Fraud/suspicion score

> How suspicious are the characteristics of the document?

### E. Straight-through-processing score

> How safe is it to allow this application to proceed without human review?

These are **not interchangeable**.

I'd explicitly put this on the whiteboard:

```text
             SCORE
               │
      ┌────────┼─────────┐
      ▼        ▼         ▼
Extraction   Validity   Risk
confidence   confidence suspicion
      │        │         │
      └────────┼─────────┘
               ▼
        Decision policy
               │
       ┌───────┼────────┐
       ▼       ▼        ▼
      STP    Review    Reject
```

That is solution thinking.

---

# 7. Now you understand why the MD mentioned confusion matrices

A confusion matrix only makes sense once you define:

**What exactly are we predicting?**

Suppose you're testing whether a document should be accepted.

Then:

| Actual  | Model accepts | Model rejects |
| ------- | ------------: | ------------: |
| Valid   |            TP |            FN |
| Invalid |            FP |            TN |

Then you can calculate:

* Precision
* Recall
* F1
* False-positive rate
* False-negative rate

But **don't stop there**.

For onboarding, the business cost of errors may be asymmetric.

For example:

> False acceptance of a fraudulent document

may be much more serious than:

> Sending a legitimate customer to manual review.

So you might deliberately accept lower automation coverage in exchange for higher precision.

That's a **business/solution decision**, not merely a model-performance decision.

---

# 8. Design experiments like a scientist

This is where you can become very useful very quickly.

Instead of saying:

> “Let's experiment with AI.”

Create hypotheses.

For example:

### Experiment 1 — Extraction accuracy

**Question**

> How accurately does Doc Studio extract PoA fields?

Dataset:

```text
500 documents
```

Ground truth:

```text
human-verified fields
```

Measure:

```text
field-level accuracy
exact match
normalized match
character error rate
```

Break down by:

* document type
* issuer
* language
* image quality
* handwritten/typed
* scan/photo
* old/new document

---

# 9. Experiment 2 — Classification

Suppose OR accepts:

```text
Bank statement
Utility bill
Government letter
Tax document
...
```

and rejects:

```text
Driving licence
Passport
random screenshot
unsupported documents
...
```

Measure:

```text
Precision
Recall
F1
Confusion matrix
```

But don't just report:

> F1 = 96%.

Report:

```text
Bank statement → 98%
Utility bill → 94%
Government letter → 91%
...
```

Because aggregate metrics can hide the actual problem.

---

# 10. Experiment 3 — Address matching

This could be particularly interesting.

Create pairs:

```text
Document address
Customer address
```

with labels:

```text
MATCH
NON-MATCH
UNCERTAIN
```

Test:

### Approach A

Exact string matching

### Approach B

Normalization + deterministic matching

### Approach C

Fuzzy matching

### Approach D

Embedding similarity

### Approach E

LLM-based structured comparison

Then compare them.

You might discover something very important:

> **A simple normalization/rules engine is already good enough for 95% of cases, while an LLM is only useful for ambiguous cases.**

That would be a much better architecture than:

> “Put every address through an LLM.”

---

# 11. Experiment 4 — LLM as an exception handler

This is one of the architectures I'd seriously investigate.

Don't do:

```text
Every document
      ↓
     LLM
```

Instead:

```text
               Document
                   ↓
              Doc Studio
                   ↓
          deterministic checks
                   ↓
          ┌────────┴────────┐
          │                 │
       obvious            ambiguous
          │                 │
          ▼                 ▼
      automate            AI/LLM
                            │
                            ▼
                       human review
```

This gives you a potential **AI escalation layer**.

For example:

```text
Confidence > 0.95
       ↓
     STP

0.70–0.95
       ↓
   AI reasoning

< 0.70
       ↓
 Human review
```

Obviously those thresholds must come from experiments rather than being invented.

---

# 12. And don't forget document fraud

This is the area I'd encourage you to explore beyond the obvious extraction task.

A proof-of-address solution isn't merely:

> OCR → address

There is potentially a **document authenticity problem**.

Questions worth investigating:

* Is the document digitally manipulated?
* Has a region of text been edited?
* Is the image generated from a screenshot?
* Is metadata suspicious?
* Is the same document being uploaded repeatedly?
* Is the document template consistent with the issuer?
* Are fonts/layouts anomalous?
* Are there signs of copy/paste?
* Does the document date make sense?
* Is the issuer legitimate?
* Does the account/name/address combination make sense?
* Are multiple applicants submitting identical documents?

You don't need to solve all of these.

But creating a **threat/risk map** would make you look at the problem much more broadly.

---

# 13. Think of the whole thing as an AI decision system

I'd draw this for the team:

```text
                         CUSTOMER
                            │
                            ▼
                       Upload PoA
                            │
                            ▼
                     ┌─────────────┐
                     │ Doc Studio  │
                     └──────┬──────┘
                            │
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
        Classification   Extraction    Quality
             │              │              │
             └──────────────┼──────────────┘
                            ▼
                    Validation Layer
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
          Policy          Matching       Risk
          checks          checks        signals
              │             │             │
              └─────────────┼─────────────┘
                            ▼
                     Decision Engine
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
             STP        AI review       Human
              │             │             │
              └─────────────┼─────────────┘
                            ▼
                       Prefill Form
                            │
                            ▼
                    Customer confirms
```

Notice where your AI engineering contribution can sit:

**not just inside Doc Studio.**

It can be the layer **around** Doc Studio.

That's potentially much more valuable.

---

# 14. The really interesting part: orchestration

Given that you've already been looking at LangChain/LangGraph/Deep Agents, I would resist the temptation to start coding an agent immediately.

First ask:

> **Does OR actually need agentic orchestration?**

There may be a legitimate use case.

For example:

```text
PoA workflow
      │
      ▼
 classify document
      │
      ├── unsupported → request another document
      │
      ▼
 extract fields
      │
      ▼
 validate fields
      │
      ├── low confidence → retry / alternative extraction
      │
      ▼
 compare customer information
      │
      ├── mismatch → investigate
      │
      ▼
 evaluate policy
      │
      ├── ambiguous → AI reasoning
      │
      ▼
 generate structured decision
```

That's a **state machine**.

And LangGraph may eventually be useful for exactly that.

But you should arrive at that conclusion from the workflow, rather than starting with:

> “I need to use LangGraph.”

That's a subtle but important difference in seniority.

---

# 15. Another thing I would introduce: a "decision contract"

For every AI component, define:

```text
INPUT
OUTPUT
CONFIDENCE
DECISION
REASON
EVIDENCE
FALLBACK
```

For example:

```json
{
  "document_type": "bank_statement",
  "document_type_confidence": 0.98,

  "customer_name": "Jane Wong",
  "customer_name_confidence": 0.99,

  "address": "123 Orchard Road #12-04",
  "address_confidence": 0.96,

  "address_match": true,
  "address_match_confidence": 0.93,

  "document_valid": true,

  "decision": "STP",
  "decision_confidence": 0.94,

  "reasons": [
    "Supported document type",
    "Document within validity period",
    "Customer name matched",
    "Address matched"
  ]
}
```

This becomes extremely useful later for:

* auditability
* debugging
* model evaluation
* human review
* compliance
* monitoring
* explaining decisions

---

# 16. Don't measure only model accuracy

This is another place you can elevate the conversation.

The ultimate metric isn't:

> "Our extraction model has 97% accuracy."

The ultimate question is:

> **"Did OR make onboarding faster and safer?"**

So I'd create metrics at several levels.

### Model metrics

```text
Precision
Recall
F1
Extraction accuracy
Calibration
Confidence distribution
```

### Workflow metrics

```text
STP rate
Human-review rate
Retry rate
Average processing time
Document rejection rate
```

### Customer metrics

```text
Completion rate
Drop-off rate
Number of uploads
Time to complete onboarding
```

### Operational metrics

```text
Manual review volume
Reviewer handling time
False rejection rate
False acceptance rate
```

### Risk metrics

```text
Fraud detection
Policy violations
Duplicate documents
Suspicious submissions
```

This gives you a **North Star for AI** rather than a collection of model experiments.

---

# 17. Build a "PoA AI evaluation framework"

This is probably the first tangible initiative I would take.

Create a document/repo:

```text
OR/
│
├── solution-design/
│   ├── poa-workflow.md
│   ├── poa-capability-map.md
│   ├── poa-risk-map.md
│   └── poa-architecture.md
│
├── evaluation/
│   ├── dataset.md
│   ├── ground-truth.md
│   ├── metrics.md
│   ├── experiments.md
│   └── results/
│
└── prototype/
    └── ...
```

You don't need permission to create a thoughtful evaluation framework.

And it gives you something concrete to bring to the team.

---

# 18. Your dataset is going to be critical

Before experimenting, ask:

> **Where are our representative documents?**

You need to think about the dataset almost like a data scientist.

Potential dimensions:

| Dimension      | Examples                     |
| -------------- | ---------------------------- |
| Document type  | bank statement, utility bill |
| Issuer         | Bank A, Bank B               |
| Language       | English, Chinese, etc.       |
| Quality        | clear, blurry, dark          |
| Format         | PDF, JPG, PNG                |
| Source         | camera, scan, screenshot     |
| Length         | 1 page, 10 pages             |
| Address format | standardized, abbreviated    |
| Name format    | exact, transliterated        |
| Validity       | current, expired             |
| Edge cases     | partial/missing fields       |
| Fraud          | genuine/suspicious           |

Then ask:

> Is our test set representative of the real customer population?

That's a much more interesting question than:

> "How many documents do we have?"

---

# 19. Think about evaluation leakage

This is another very DS/AI-engineering way to contribute.

Suppose you test 1,000 documents.

But the same bank statement template appears 300 times.

Your model may look amazing because it has effectively seen the same pattern repeatedly.

So think about:

```text
Train
Validation
Test
```

and potentially split by:

* issuer
* document template
* time period
* customer
* document type

depending on what you're evaluating.

You want to know:

> **Does this generalize to documents we haven't seen?**

---

# 20. Think about calibration, not just accuracy

If your system says:

```text
Confidence = 95%
```

does it actually get things right approximately 95% of the time?

That's **calibration**.

This becomes especially important if your architecture says:

```text
confidence > threshold → automated
confidence < threshold → human
```

Otherwise the confidence score isn't really useful as a decision signal.

This is exactly the kind of experiment I would expect an AI engineer contributing to solution design to propose.

---

# 21. Consider a champion/challenger setup

Once Doc Studio gives you a baseline:

```text
                Doc Studio
                   │
                   ▼
                Baseline
```

you can test alternatives:

```text
Doc Studio
    │
    ├── Baseline
    │
    ├── Alternative model
    │
    ├── LLM extraction
    │
    └── Hybrid approach
```

Then evaluate them on exactly the same test set.

This prevents:

> "Model X feels better."

Instead:

> "On the same 1,000-document evaluation set, approach A produced X, approach B produced Y, with these trade-offs."

That's how you become useful to the MD.

---

# 22. Think beyond PoA

Here's where I think your OR opportunity gets bigger.

PoA is probably just **one document journey inside customer onboarding**.

There may be:

```text
Identity document
Proof of address
Tax document
Employment document
Bank statement
Corporate documents
...
```

You can start seeing a common architecture:

```text
                OR Document Intelligence Layer
                           │
        ┌──────────────────┼─────────────────┐
        ▼                  ▼                 ▼
   Classification     Extraction       Verification
        │                  │                 │
        └──────────────────┼─────────────────┘
                           ▼
                     Decision Engine
                           │
                           ▼
                     Human Review
```

If you can establish the pattern for PoA, you potentially create a reusable architecture for OR.

**That's much more strategic than owning one PoA model.**

---

# 23. There is also a "document → customer profile" problem

Eventually, you're not merely extracting fields.

You're constructing a customer representation.

For example:

```text
Customer
  │
  ├── Name
  ├── DOB
  ├── Address
  ├── Nationality
  ├── Employment
  └── Tax information
```

Documents provide evidence:

```text
Bank statement ──────┐
Utility bill ────────┤
Government letter ───┤
Identity document ───┤
                     ▼
              Customer Profile
                     │
                     ▼
                Validation
```

This introduces **cross-document consistency checking**.

For example:

> ID says one name.

> Bank statement says another.

> PoA says a third address.

Now you're solving a much richer problem:

**entity resolution + evidence aggregation + consistency checking.**

That could become a major AI capability for OR.

---

# 24. And think about explainability

Imagine a reviewer receives:

> **Manual review required**

Don't give them:

> Score: 0.63

Give them:

```text
MANUAL REVIEW REQUIRED

Reasons:
✓ Document classified as bank statement
✓ Document date is within permitted period
✓ Customer name matched
⚠ Address similarity: 71%
⚠ Unit number could not be confidently extracted
⚠ Document quality: moderate

Recommended action:
Verify customer's unit number.
```

That's an actual operational product.

AI shouldn't just produce a number.

It should produce **evidence that supports the decision**.

---

# 25. What I would do in your shoes this week

Don't try to solve everything.

I'd make these **five deliverables**.

### Deliverable 1 — PoA journey

One-page flow:

```text
Upload
 ↓
Classify
 ↓
Extract
 ↓
Validate
 ↓
Match
 ↓
Risk
 ↓
Decision
 ↓
Prefill
 ↓
Customer confirmation
```

---

### Deliverable 2 — Doc Studio capability map

For every stage:

```text
What does Doc Studio already provide?
What doesn't it provide?
What APIs/data does it expose?
What confidence information does it provide?
```

---

### Deliverable 3 — AI experiment plan

Something like:

```text
Experiment 1: document classification
Experiment 2: field extraction
Experiment 3: address normalization
Experiment 4: address matching
Experiment 5: confidence calibration
Experiment 6: human-vs-AI review
Experiment 7: LLM as exception handler
```

---

### Deliverable 4 — Evaluation dataset proposal

Define:

```text
What data?
How many?
What labels?
What edge cases?
What ground truth?
How will we split it?
What metrics?
```

---

### Deliverable 5 — Architecture proposal

Something like:

```text
                    OR
                     │
              Document upload
                     │
                     ▼
                Doc Studio
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   Classification Extraction   Quality
        │            │            │
        └────────────┼────────────┘
                     ▼
              AI Validation
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
     Policy       Matching       Risk
                     │
                     ▼
              Decision Engine
                     │
           ┌─────────┼─────────┐
           ▼         ▼         ▼
          STP       AI        Human
                   review      review
                     │
                     ▼
                  Prefill
```

Don't present this as **the answer**.

Present it as:

> "This is my current 60%-formed view. I'd like us to challenge it."

That fits your preferred leadership style extremely well.

---

# 26. How I would approach your two fellow engineers

This part matters because of the OR dynamics you've told me about.

Don't say:

> "I'm leading the PoA work."

Instead:

> **"I've been thinking about how we can structure the PoA problem. I sketched an initial flow and some experiments. I'd like to get your input, especially on what Doc Studio already supports and what you've seen from the existing work."**

Then give them ownership.

For example:

**Engineer A**

> Can you investigate Doc Studio capabilities and limitations?

**Engineer B**

> Can you look into the evaluation dataset / existing onboarding requirements?

You own the **integration of those pieces into the solution**.

That's leadership without announcing leadership.

---

# 27. Your next conversation with the MD

I would actually go to the MD relatively soon.

Not with:

> "What should I work on?"

Instead:

I've been thinking about the proof-of-address journey and I want to take a more structured approach rather than jumping straight into implementation.

My current thinking is to map the end-to-end journey first — document classification, extraction, validation, address matching, risk/decisioning, and prefill — and then identify what Doc Studio already gives us versus where we may need additional AI capabilities.

I'd also like to build an evaluation framework around representative documents, ground truth, confusion matrices and threshold analysis, so that we can make decisions based on experiments rather than assumptions.

I'd like to use PoA as a concrete starting point and see whether the architecture can eventually generalize to other onboarding documents.

This is still my 60%-formed view, so I'd like to get your perspective on whether this is the right direction and what business outcomes you would most want us to optimize for.

That is a **very different conversation** from:

> "What task can I take?"

---

# 28. One thing I especially want you to avoid

Don't disappear for two weeks and come back with:

> "I built an agent."

That's a trap.

Especially in a complex project with unclear requirements.

You could build something technically impressive that solves the wrong problem.

Instead, operate in this loop:

```text
                 ┌──────────────┐
                 │ Business     │
                 │ problem      │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │ Hypothesis   │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │ Experiment   │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │ Evidence     │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │ Architecture │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │ Prototype    │
                 └──────┬───────┘
                        ↓
                 ┌──────────────┐
                 │ Evaluation   │
                 └──────┬───────┘
                        │
                        └──────→ iterate
```

**That's how I would want you to operate as an FDE.**

---

# 29. Your real AI contribution to OR

If I zoom out, I don't think your role should be:

> **"The person who builds the PoA AI."**

I'd aim for:

> **"The AI engineer who helps OR figure out how AI should actually be used in the onboarding journey."**

That means you gradually become the person who can answer:

* Where should AI be used?
* Where should deterministic rules be used?
* What should Doc Studio own?
* Where do we need additional models?
* Where is an LLM actually justified?
* What needs human review?
* What should be automated?
* How do we evaluate it?
* How do we know the score is trustworthy?
* How do we monitor it in production?
* How do we handle exceptions?
* How do we make the system auditable?
* How do we generalize the architecture to other onboarding documents?

**That is solution design.**

And it also gives you a natural way to lead the other two engineers: you're not competing with them for who gets to write the most code. You're creating the **problem structure** within which all three of you can contribute.

### If I were sitting beside you Monday morning

I would literally open a blank document and put these seven headings in it:

```text
OR — Proof of Address AI Solution

1. Business Objective
2. Current Customer Journey
3. Doc Studio Capabilities
4. AI / Non-AI Decision Points
5. Evaluation & Experiment Plan
6. Target Architecture
7. Open Questions / Decisions Needed
```

Then start filling it in.

**Don't wait for someone to assign you the next task. Create the questions that the team needs answered.**

That is probably the most important shift for you in OR.
