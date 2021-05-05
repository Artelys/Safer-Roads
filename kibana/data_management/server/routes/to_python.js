export default function (server) {
	server.route({
    path: '/api/data_management/to_python',
    method: 'POST',
    handler: async function(req, reply) {
    	console.log(req.payload.path_python);
    	console.log("new promise");
		const request = require('request');
		var res = await new Promise((resolve, reject) => {
        	request('http://flask:5000/'+req.payload.path_python, 
        		{method: 'POST', "headers":{"Content-Type": "application/json"}, body: JSON.stringify(req.payload)},
        		(error, response, body) => {
       				
				console.log(error, response, body);
		            if (error) reject(error);
		            if(typeof(response)=="undefined"){   
					  reject(error);   
					}
		            else if (response.statusCode != 200) {
		                reject('Invalid status code <' + response.statusCode + '>');
		            }
		            else{
						res = response.body;
			            resolve(res);
		            }
		            
	            
	        });
	    });
		return res;
    	}
	})
}
