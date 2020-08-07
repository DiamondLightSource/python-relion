class CTFFind:
    def __init__(self, path):
        self.basepath = path
        print("CTFFind created!")

    def __str__(self):
        return f"I'm a CTFFind instance at {self.basepath}"

    @property
    def plate_colour(self):
        return "green"
