class Singleton:
    """
    A base class that implements the Singleton design pattern.

    The Singleton pattern ensures that only one instance of a class is created
    throughout the application's lifecycle. This is useful for classes where
    having multiple instances would cause conflicts or inefficiency, such as
    database connection classes, services, or repositories.

    Attributes:
        _instances (dict): A dictionary that stores the single instance of each class
                           that inherits from Singleton. The key is the class itself,
                           and the value is the class instance.
    """

    _instances = {}

    def __new__(cls, *args, **kwargs):
        """
        Controls the instantiation of classes that inherit from Singleton. If an instance
        of the class doesn't already exist, it creates and stores a new one. Otherwise,
        it returns the existing instance from the _instances dictionary.

        Parameters:
            *args: Variable-length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            object: The single instance of the class.
        """
        # Check if the class already has an instance stored in _instances
        if cls not in cls._instances:
            # Create a new instance using the default __new__ method (without extra args)
            cls._instances[cls] = super().__new__(cls)

        # Return the existing or newly created instance
        return cls._instances[cls]
