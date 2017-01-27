# Azure Text Analytics filter plugin for Embulk

Azure Text Analytics filter plugin for Embulk.

## Azure Text Analytics Documentation

* [Microsoft Cognitive Services \- Documentation](https://www.microsoft.com/cognitive-services/en-us/text-analytics/documentation)
* [azure\-docs/cognitive\-services\-text\-analytics\-quick\-start\.md at master · Microsoft/azure\-docs](https://github.com/Microsoft/azure-docs/blob/master/articles/cognitive-services/cognitive-services-text-analytics-quick-start.md)

## Overview

* **Plugin type**: filter

## Configuration

- **api_type**: api_type(string),
- **language**: language(string, default: nil),
- **out_key_name**: out_key_name(string),
- **key_name**: key_name(string),
- **body_params**: body_params(hash, default: {}),
- **params**: params(hash, default: {}),
- **delay**: delay(integer, default: 0),
- **per_request**: per_request(integer, default: 1),
- **bulk_size**: bulk_size(integer, default: 100),
- **subscription_key**: subscription_key(string),

## Example
### sentiment

```yaml
  # en,es,fr,pt
  - type: azure_text_analytics
    api_type: sentiment
    key_name: target_key
    out_key_name: target_key_sentiment
    language: en
    delay: 2
    subscription_key: XXXXXXXXXXXXXXXXXXXXXXXXXXX
```

* sentiment support language
  * en
  * es
  * fr
  * pt


### languages

```yaml
  - type: azure_text_analytics
    api_type: languages
    out_key_name: target_key_languages
    language: en
    key_name: target_key
    delay: 2
    subscription_key: XXXXXXXXXXXXXXXXXXXXXXXXXXX
```

### keyPhrases

```yaml
  - type: azure_text_analytics
    api_type: keyPhrases
    out_key_name: target_key_keyPhrases
    key_name: target_key
    delay: 2
    subscription_key: XXXXXXXXXXXXXXXXXXXXXXXXXXX
```

## Build

```
$ rake
```
