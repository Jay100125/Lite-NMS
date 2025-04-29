
### `POST /provision/:id` — Provision Monitor

**Request Body:**
```json
{
  "IP": [2,3]
}
```
**Success (200):**
```json
{
  "status": "success",
  "monitorId": "[1,2]"
}
```

**Failure (400):**
```json
{
  "status": "failed",
  "error": "<error message>"
}
```


### `GET /provision/:id` — Get Provision By ID
**Success (200):**
```json
{
  "status": "success",
  "data": "{
      "id": "",
      "type": "",
      "credential_id": "",
      "ip": "",
      "port": 123,
      "metric": []
  }"
}
```

### `DELETE /provision/:id` — Delete Provision
**Success (200):**
```json
{ "status": "success", "deletion": "success" }
```

**Failure (404):**
```json
{
  "status": "failed",
  "deletion": "failed",
  "error": "No provision found"
}
```
**Failure (500):**
```json
{
  "status": "failed",
  "deletion": "failed",
  "error": "internal server error"
}
```

### `GET /provision` — Get All Provisions
**Success (200):**
```json
{
  "status": "success",
  "data": [
    {
      "id": "",
      "type": "",
      "credential_id": "",
      "ip": "",
      "port": 123,
      "metric": []
    }
  ]
}
```

**Failure (500):**
```json
{
  "status": "failed",
  "error": "internal server error"
}
```

###
