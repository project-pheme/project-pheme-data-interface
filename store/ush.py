from ushapy import ushahidiv2 as v2

class UshahidiLink(object):
    def ensure_category(self, **kwargs):
        raise Error("Abstract class")
    def ensure_reporter(self, **kwargs):
        raise Error("Abstract class")
    def ensure_report(self, **kwargs):
        raise Error("Abstract class")
    def ensure_message(self, **kwargs):
        raise Error("Abstract class")

class UshahidiLinkV2(UshahidiLink):
    def __init__(self, url='', username='', password=''):
        self.url = kwargs['url']
        self.username = kwargs['username']
        self.password = kwargs['password']
        self._load_contents()

    def ensure_category(self, **kwargs):
        # Check if category exists?
        v2.add_category_to_map(self.url, self.username, self.password, **kwargs)

    def ensure_reporter(self):
        # Check if reporter exists?
        v2.add_reporter_to_platform()

    def ensure_report(self):
        # Check if report exists?
        v2.add_report_to_platform()

    def ensure_message(self):
        v2.add_message_to_platform()
        pass

    def _load_contents(self):
        # Initialize content cache
        self.categories = get_all_categories(self.url)
