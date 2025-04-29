
## Credentials API

Manages credentials used for accessing devices or services.

### `POST /api/credential` — Create Credential

**Request Body:**
```json
{
  "credential name": "",        // Required: Unique name
  "sys_type": "",             // windows/linux/snmp
  "cred_data": {
    "user": "string",            // Required for "ssh"/"winrm"
    "password": "string"        // Required for "ssh"/"winrm"
      }
}
```

**Success (201):** created credential profile
```json
{
  "msg": "success",
  "credential_profile_id": 1
}
```

**Failure (409):** duplicate name
```json
{
  "msg": "failed",
  "error": "Duplicate cred name"
}
```

**Failure (400):** validation error/empty field
```json
{
  "msg": "failed",
  "error": "missing field or invalid data"
}
```

**Failure (500):** Internal server error
```json
{
  "msg": "failed",
  "error": "internal server error"
}
```
---

### `PATCH /api/credential/:id` — Update Credential

**Request Body:**
```json
{
  "credential name": "",
  "sys_type": "",
  "cred_data": {
    "user": "string",
    "password": "string"
  }
}
```

**Success (200):**
```json
{ "update": "success" }
```

**Failure (409):** duplicate name
```json
{
  "msg": "failed",
  "error": "Duplicate cred.name"
}
```

**Failure (400):** validation error/empty field
```json
{
  "msg": "failed",
  "error": "missing field or invalid data"
}
```

**Failure (500):** Internal server error
```json
{
  "msg": "failed",
  "error": "internal server error"
}
```

---

### `GET /api/credential` — Retrieve All Credentials

**Success (200):**
```json
{
  "result":  {
    "credential_profiles" : [
      {
        "id": "",
        "name": "",
        "type": "",
        "credentials": {
          "username": "",
          "password": ""
        }
      }, ...
    ]
  }
}
```

**Failure (404):** profile doesn't exist
```json
{
  "status": "failed",
  "msg": "No credential found"
}
```

**Failure (500):** internal server error
```json
{
  "status": "failed",
  "msg": "internal server error"
}
```
---

### `GET /api/credential/:id` — Retrieve Credential by ID

**Request Body:** NONE

**Success (200):**
```json
{
  "result":  {
        "id": "",
        "name": "",
        "type": "",
        "credentials": {
          "username": "",
          "password": ""
        }

  }
}
```

**Failure (404):** profile doesn't exist
```json
{
  "status": "failed",
  "msg": "No credential found"
}
```

**Failure (500):** internal server error
```json
{
  "status": "failed",
  "msg": "internal server error"
}
```

---

### `DELETE /api/credential/:id` — Delete Credential

**Request Body:** NONE
**Success (200):**
```json
{
  "status": "success"
}
```

**Failure (400):** invalid format
```json
{
  "deletion": "failed",
  "error": "Wrong ID"
}
```

**Failure (404):** Id doesn't exist
```json
{
  "deletion": "failed",
  "error": "ID doesn't exist"
}
```

**Failure (409):** credential in use
```json
{
  "deletion": "failed",
  "error": "Credential in use"
}
```

**Failure (500):** internal server error
```json
{
  "deletion": "failed",
  "msg": "internal server error"
}
```


---

