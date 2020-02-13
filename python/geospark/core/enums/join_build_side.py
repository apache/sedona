from geospark.utils.decorators import classproperty


class JoinBuildSide:

    @classproperty
    def LEFT(self):
        return "LEFT"

    @classproperty
    def RIGHT(self):
        return "RIGHT"