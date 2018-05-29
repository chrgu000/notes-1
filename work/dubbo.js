const nzd=require('node-zookeeper-dubbo');
const app=require('express')();
const opt={
  application:{name:'dubboService'},
  register:'localhost:2181',
  dubboVer:'2.5.3',
  root:'dubbo',
  dependencies:{
    Foo:{
      interface:'cn.unicom.gx.demo.export.interfaces.FastTestServExt',
      //version:'LATEST',
      timeout:6000,
      group:'UNICOM',
      methodSignature: {
        callMe : (id) => [ {'$class': 'net.sf.json.JSONObject', '$': id} ],
        
      }
    }
  }  
}
opt.java = require('js-to-java')

const Dubbo=new nzd(opt);

const customerObj = {
  
    a: 1,
    b: 'test',
    c: {$class: 'java.lang.Long', $: 123}
 
};

app.get('/foo',(req,res)=>{
  Dubbo.Foo
    .callMe(customerObj)
    .then(data=>res.send(data))
    .catch(err=>res.send(err))
})



app.listen(9090)
