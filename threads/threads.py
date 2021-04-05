from threading import Thread

COUNTDOWN = 5

class Th(Thread):

    def __init__ (self, num):
        print("Making Thread number " + str(num))
        Thread.__init__(self)
        self.num = num
        self.countdown = COUNTDOWN

    def run(self):
        while (self.countdown):
            print("Thread " + str(self.num) + " (" + str(self.countdown) + ")")
            self.countdown -= 1

# ---

for thread_number in range (5):
    thread = Th(thread_number)
    thread.start()