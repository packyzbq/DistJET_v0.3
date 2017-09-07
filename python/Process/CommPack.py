import types


class CommPack:
    def __init__(self,command):
        self.command_list = []
        if type(command) == types.StringType:
            self.command_list.append(command)
        elif type(command) == types.ListType:
            self.command_list.extend(command)
        self.current = 0

    def next_comm(self):
        self.current+=1
        return self.command_list[self.current]

    def has_next(self):
        return self.current < len(self.command_list)