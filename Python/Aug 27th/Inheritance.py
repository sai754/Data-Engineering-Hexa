# Base class (parent class)

class Animal:
    def __init__(self,name): # Constructor
        self.name = name

    def speak(self):
        pass

# Derived class
class Dog(Animal):
    def speak(self):
        return "Woof"

class Cat(Animal):
    def speak(self):
        return "Meow"

dog = Dog("Buddy")
cat = Cat("Pichu")

print(f"{dog.name} says {dog.speak()}")
print(f"{cat.name} says {cat.speak()}")


class Payment:
    def __init__(self, name):
        self.name = name

    def GetPayment(self):
        pass

class GPay(Payment):
    def GetPayment(self):
        print("Speaking to Google Payment Gateway")

class PhonePay(Payment):
    def GetPayment(self):
        print("Speaking to phone pay gateway")

class AmazonPay(Payment):
    def GetPayment(self):
        print("Speaking to Amazon Gateway")

gpay = GPay("Gpay")
gpay.GetPayment()

phonepay = PhonePay("PhonePay")
phonepay.GetPayment()
