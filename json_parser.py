from concurrent.futures.process import ProcessPoolExecutor


class QueryLauncher:
    def __init__(self):
        self.route_list = []
        self.rule_list = []
        self.queried_report = {}

    def route(self, path):
        def decorator(rule):
            self.route_list.append((rule, path))
            return rule
        return decorator

    def query(self, report):
        payloads = [(rule, path, report) for rule, path in self.route_list]
        with ProcessPoolExecutor() as executor:
            results = executor.map(self._execute_query, payloads)
            for result in results:
                for k, v in result.items():
                    self.queried_report[k] = v
        return self

    def _execute_query(self, payload):
        def find_node(path, report):
            root = report
            nodes = path.split('/')
            for node in nodes:
                if isinstance(root, list):
                    root = root[int(node)]
                else:
                    root = root.get(node)
                if root is None:
                    return None
            return root
        children = []
        rule, path, report = payload
        key = f'{rule.__name__}'
        parent = find_node(path, report)
        if parent:
            if isinstance(parent, list):
                for i in range(len(parent)):
                    child = rule(parent[i], report)
                    if child:
                        children.append(child)
            else:
                child = rule(parent, report)
                if child is not None:
                    children.append(child)
        return {key: children}

    def register(self):
        def decorator(rule):
            self.rule_list.append(rule)
            return rule
        return decorator

    def launch(self):
        assert self.queried_report
        rendered_report = {}
        payloads = [(rule, self.queried_report) for rule in self.rule_list]
        with ProcessPoolExecutor() as executor:
            results = executor.map(self._launch_rules, payloads)
            for result in results:
                for k, v in result.items():
                    rendered_report[k] = v
        return rendered_report

    def _launch_rules(self, payload):
        rule, report = payload
        result = rule(report)
        return result
