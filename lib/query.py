class Query:
    def get_attribs(self, config, key):
        if key not in config.keys() or config[key] == "":
            attrib = None
        elif key == "tags":
            attrib = self.get_tags(config[key])
        else:
            attrib = config[key]
        return attrib
    
    def get_tags(self, tags):
        tags = tags if type(tags) != list else ('.'.join(tags))
        return tags

    def __init__(self, config):
        self.platform = self.get_attribs(config, "platform")
        self.category = self.get_attribs(config, "category")
        self.sortby = self.get_attribs(config, "sort-by")
        self.tags = self.get_attribs(config, "tags")
        self.game_id = self.get_attribs(config, "id")
    
    def url(self):
        query_string = ''
        if self.platform or self.category or self.sortby or self.tags or self.game_id:
            query_string = '?'
            query_param_list = []
            if self.platform:
                query_param_list.append(f"platform={self.platform}")
            if self.category:
                query_param_list.append(f"category={self.category}")
            if self.sortby:
                query_param_list.append(f"sort-by={self.sortby}")
            if self.tags:
                query_param_list.append(f"tag={self.tags}")
            if self.game_id:
                query_param_list.append(f"id={self.game_id}")
            query_string = query_string + '&'.join(query_param_list)
        return query_string