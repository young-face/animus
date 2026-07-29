# Key-Value Storage

This is a core Animus component. It provides key-value storage that can be accessed both locally and remotely.

## Quick Explanation

KVS has its main abstraction `KeyValueRow`, that represents a single key-value record. It assumes that any value can be identified by a composite key consisting of `namespace`, `name` and `key`. These terms are just general names of the use-cases in which they can be used. Let's look at examples of such use-cases.

###### Flattening multiple configuration files

Say, we have to represent all of the keys in each file in our file tree. Then, if we had a CSV it would look like this:

```csv
namespace,name,key,value
path/to/file,file.json,nested.field[1],My value
```

In this scenario `namespace` means path to the file, `name` uniquely identifies that file in the namespace and `key` points to the specific property in the file.

###### Flattening data models

Imagine you have a data model that has such entities as `User`, `Order` and `Payment`. In this scenario we can build the same CSV that contains all of the data of any entity in our system.

```csv
namespace,name,key,value
User,5b2ec5d5-f1a3-4592-bb51-a15fc893cbc7,name,Alexander
User,5b2ec5d5-f1a3-4592-bb51-a15fc893cbc7,last_name,Plotnikov
Order,41c0cd0c-cda1-4a71-9cfb-278a69e43053,status,SHIPPING
Order,41c0cd0c-cda1-4a71-9cfb-278a69e43053,address,Earth
Payment,cc95547e-8fda-4b26-89bf-09530efaf4f1,amount,100.00
Payment,cc95547e-8fda-4b26-89bf-09530efaf4f1,COMPLETED,100.00
```

Here, `namespace` will represent _a class of an object_, `name` will identify specific object and `key` will hold the name of some property.
