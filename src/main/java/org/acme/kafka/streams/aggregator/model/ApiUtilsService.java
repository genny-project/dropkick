package org.acme.kafka.streams.aggregator.model;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@Path("/utils")
@RegisterRestClient
public interface ApiUtilsService {

	@GET
	@Path("/realms")
	@Produces("application/json")
	String getRealms(
			@HeaderParam("Authorization") final String bearertoken);


}
