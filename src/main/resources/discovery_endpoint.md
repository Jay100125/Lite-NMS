## Discovery API

Handles configuration and execution of discovery.

### `POST /api/discovery` — Create Discovery Configuration

**Request Body:**
```json
{
  "discovery name": "",
  "cred.profile.id": "",
  "ip.address": "range or single",
  "port": 22
}
```

**Success (200):**
```json
{
  "insertion": "success",
  "discovery_id": "<id>"
}
```

**Failure (409):** Duplicate name
```json
{
  "insertion": "failed",
  "error": "Duplicate discovery name"
}
```

**Failure (400):** Validation error/empty field
```json
{
  "insertion": "failed",
  "error": "missing field or invalid data"
}
```

**Failure (500):** Internal server error
```json
{
  "insertion": "failed",
  "error": "internal server error"
}
```

---

### `patch /discovery/:id` — Update Discovery

**Request Body:**
```json
{
  "discovery name": "",
  "ip.address": "",
  "port": 1234,
  "cred.profile.id": ""
}
```

**Success (200):**
```json
{
  "update": "success"
}
```

**Failure (400):** validation error/empty field
```json
{
  "update": "failed",
  "error": "missing data or invalid data"
}
```

**Failure (409):** Duplicate name
```json
{
  "update": "failed",
  "error": "Duplicate discovery name"
}
```

**Failure (500):** Internal server error
```json
{
  "update": "failed",
  "error": "internal server error"
}
```

---

### `GET /api/discovery` — Retrieve All Discoveries

**Success (200):**
```json
{
  "status": "success",
  "result": "[{
    "discovery name": "",
    "cred.profile.id": "",
    "ip.address": "",
    "port": 22,
    "reachable_ip": []
  },....]""

}
```

**Failure (404):** No discoveries found
```json
{
  "status": "failed",
  "error": "No discoveries found"
}
```
**Failure (500):** Internal server error
```json
{
  "status": "failed",
  "error": "internal server error"
}
```

---

### `GET /api/discovery/:id` — Retrieve Discovery by ID

**Success (200):**
```json
{
  "status": "success",
  "result": "{
      "discovery name": "",
      "cred.profile.id": "",
      "ip.address": "",
      "port": 22,
      "reachable_ip": []
      }"
}
```

**Failure (404):** No discoveries found
```json
{
  "status": "failed",
  "error": "No discoveries found"
}
```
**Failure (500):** Internal server error
```json
{
  "status": "failed",
  "error": "internal server error"
}
```

---

### `DELETE /discovery/:id` — Delete Discovery

**Request Body:** NONE
**Success (200):** deletion successful
```json
{
  "status": "success"
}
```

**Failure (404):** discovery not found
```json
{
  "status": "failed",
  "error": "No discovery found"
}
```

**Failure (500):** Internal server error
```json
{
  "deletion": "failed",
  "error": "internal server error"
}
```

---

### `POST /discovery/:id/run` — Run Discovery

**Request Body:** NONE
**Success (200):**
```json
{
  "status": "success",
  "Successful_Discovered_ip" : ["ip1", "ip2", "ip3"],
  "Failed_ip" : ["ip4", "ip5"]
}
```

**Failure (400):**
```json
{
  "status": "failed",
  "error": "<error message>" }
```

---
