
def unpack_stream_descriptor(desc):
    """
    Returns dicts for tags and annotations found in supplied stream
    """
    tags = {}
    for tag in desc.tags:
        tags[tag.key] = tag.value
    anns = {}
    for ann in desc.annotations:
        anns[ann.key] = ann.value
    return tags, anns
