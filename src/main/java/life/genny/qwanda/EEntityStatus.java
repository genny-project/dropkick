package life.genny.qwanda;

public enum EEntityStatus {

	// This enum is designed for simplicity and speed. 
	// The database will be expected to default to ACTIVE initially and we'll slowly bring in theother statuses

	ACTIVE,          // 0 - This status means that the entity should be picked up in all database fetches
	TEST,            // 1 - This means that the entity is a well crafted test entity for use by devs
	TEMPORARY,       // 3 - This means that the entity is a useful and persistent test object
	PENDING,         // 4 - This means that the entity is currently under construction and cannot be fetched under normal operation
	DISABLED,        // 5 - This means that the entity is currently not to be fetched, but is expected to possibly be returned to active status
	ARCHIVED,        // 6 - This means that the entity is not to be deleted , but is archived but not visible to normal operation
	PENDING_DELETE,  // 7 - This means that the entity is pending deletion, but not yet deleted
	DELETED,         // 8 - This means that the entity is marked deleted
	
}