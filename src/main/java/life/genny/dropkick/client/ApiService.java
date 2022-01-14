package life.genny.dropkick.client;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@Path("/service")
@RegisterRestClient
public interface ApiService {

	@GET
	@Path("/cache/read/{realm}/{key}")
	@Produces("application/json")
	String getDataFromCache(@PathParam("realm") String realm, @PathParam("key") String key,
			@HeaderParam("Authorization") final String bearertoken);

	@POST
	@Path("/cache/write/{key}")
	@Produces("application/json")
	String writeDataIntoCache(@PathParam("key") String key, final String value,@HeaderParam("Authorization") final String bearertoken);

}
