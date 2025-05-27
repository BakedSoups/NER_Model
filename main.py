import json
import spacy

nlp = spacy.load("en_core_web_sm")

original_sentence = "Barack Obama was the 44th President of the United States. He was born in Hawaii. Obama served two terms."

# loading harry's entity reference data from file
with open("entities.json", "r") as f:
    entity_data = json.load(f)

doc_sents = list(nlp.pipe([sent.text for sent in nlp(original_sentence).sents]))

associations = {}

main_entity = "Barack Obama"
main_refs = entity_data.get(main_entity, {}).get("references", [])

for ref in main_refs:
    sent_index, _, _ = ref
    sent = doc_sents[sent_index]

    # Simple rule-based extraction (can be improved)
    if sent_index == 0:
        if "President" in sent.text:
            associations.setdefault(main_entity, []).append("44th President")
    elif sent_index == 1:
        associations.setdefault(main_entity, []).append("born in Hawaii")
    elif sent_index == 2:
        associations.setdefault(main_entity, []).append("served two terms")

# Output as JSON
with open("entity_associations.json", "w") as f:
    json.dump(associations, f, indent=2)

# print result for debuging
print(json.dumps(associations, indent=2))
