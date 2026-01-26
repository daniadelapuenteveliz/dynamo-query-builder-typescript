import { DynamoClient, KeySchema, Table, QueryParams } from '../index';

const tableName = "sam-smith-KheprionAi-dev-actorTeam";
const client = new DynamoClient({});

type pk = {
    organization_id: string;
};

type sk = {
    scope_id: string;
    user_id: string;
    team_id: string;
};

type data = {
    "scope_id#team_id#user_id": string;
    created_at: string;
    updated_at: string;
};

const keySchema: KeySchema = {
    pk: {
        name: 'organization_id',
        keys: ['organization_id'],
        separator: '#'
    },
    sk: {
        name: 'scope_id#user_id#team_id',
        keys: ['scope_id', 'user_id', 'team_id'],
        separator: '#'
    },
    indexes: {
        "scope_team_user": {
            pk: {
                name: 'organization_id',
                keys: ['organization_id'],
                separator: '#'
            },
            sk: {
                name: 'scope_id#team_id#user_id',
                keys: ['scope_id', 'team_id', 'user_id'],
                separator: '#'
            }
        }
    }
};

const actorTeamTable: Table<pk, sk, data> = client.table<pk, sk, data>(tableName, keySchema);

export interface QueryActorTeamParams {
    organizationId: string;
    index?: string;
    scopeId?: string;
    userId?: string;
    teamId?: string;
    limit?: number;
    pagination?: {
        pivot: {
            organization_id: string;
            scope_id: string;
            user_id: string;
            team_id: string;
        };
        direction: 'forward' | 'backward';
    };
}

export async function queryIndex(params: QueryActorTeamParams) {
    const organizationId = params.organizationId;
    const qparams: QueryParams<pk, data> = {
        pk: { organization_id: organizationId },
        limit: params.limit || 10,
        IndexName: "scope_team_user"
    };
    if (params.pagination) {
        qparams.pagination = {
            direction: params.pagination.direction,
            pivot: {
                organization_id: params.pagination.pivot.organization_id,
                scope_id: params.pagination.pivot.scope_id,
                user_id: params.pagination.pivot.user_id,
                team_id: params.pagination.pivot.team_id
            }
        }
    }
    console.log("aksdklsñkdfljdlsdjfdskjld");
    let query = actorTeamTable.query(qparams);
    console.log("aksdklsñkdfljdlsdjfdskjld 2");
    if (params.scopeId && params.teamId && params.userId) {
        const sk = {
            scope_id: params.scopeId,
            team_id: params.teamId,
            user_id: params.userId
        };
        query = query.whereSKBeginsWith(sk);
    } else if (params.scopeId && params.teamId) {
        console.log("aksdklsñkdfljdlsdjfdskjld 3");
        const sk = {
            scope_id: params.scopeId,
            team_id: params.teamId
        };
        query = query.whereSKBeginsWith(sk);
    } else if (params.scopeId) {
        const sk = {
            scope_id: params.scopeId,
        };
        query = query.whereSKBeginsWith(sk);
    }
    console.log("aksdklsñkdfljdlsdjfdskjld 4");
    if (
        !qparams.pagination ||
        !qparams.pagination.direction ||
        qparams.pagination.direction === 'forward'
    ) {
        query.sortAscending();
    } else {
        query.sortDescending();
    }
    if (qparams.pagination && qparams.pagination.pivot) {
        query.pivot(qparams.pagination.pivot);
    }
    console.log("asñkldñalkslkasdkls");
    return await query.run();
}


async function main() {
    const params: QueryActorTeamParams = {
        index: "scope_user_team",
        organizationId: "daniaspa",
        teamId: "team1",
        scopeId: "daniaspa",
        limit: 3
    };

    try {
        const result = await queryIndex(params);
        console.log(JSON.stringify(result, null, 2));
    } catch (error) {
        console.error(error);
    }
}

main();



//acá