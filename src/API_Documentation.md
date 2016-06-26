**This documentation is automatically generated.**

**Output schemas only represent `data` and not the full output; see output examples and the JSend specification.**

# /api/event

    Content-Type: application/json



<br>
<br>

# /api/event/capture

    Content-Type: application/json



<br>
<br>

# /api/event/capture/\(?P\<datachannel\_id\>\[a\-zA\-Z0\-9\_\\\-\]\+\)/?$

    Content-Type: application/json



<br>
<br>

# /api/event/import

    Content-Type: application/json

## POST


**Input Schema**
```json
{
    "additionalProperties": false,
    "properties": {
        "dataChannelId": {
            "type": "string"
        },
        "source": {
            "type": "string"
        }
    },
    "required": [
        "source",
        "dataChannelId"
    ],
    "type": "object"
}
```



**Output Schema**
```json
null
```



**Notes**

POST an event to be created from an existing data channel

* `source`: set to "capture"
* `dataChannelId`: id of the data channel to be imported



<br>
<br>

# /api/stories/\(?P\<story\_id\>\[a\-zA\-Z0\-9\_\\\-\]\+\)

    Content-Type: application/json


