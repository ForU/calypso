# Model vs Table

## roles

> Model as data container, Table as data generator
So you can create Model any time, and Table only need to be created once.

## interface friendly

+ if all operation is based on model, every time you need a action, you need to
create an instance of the model (of cause you can make the the operation as
@`classmethod`, but that just make more confusing other than clear roles)

Q. So what if no such Model or Table, only Model (ability of table is merged into
Model)?
A. app code would be:
```
m = Model()
m.a = 1
m.b = 'ok'
...

m.operate()           # no need to pass to m as parameter of operate

```
Or
```
m = Model()           # need create instance
data = { m.a: 1, m.b:'ok' }

m.operate( data )     # insert, update, delete, select
```


## performance


## 
