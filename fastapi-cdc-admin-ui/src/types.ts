export interface Client {
    id: number;
    client_name: string;
    login_url: string;
    oauth_grant_type: 'password' | 'client_credentials';
    oauth_client_id: string;
    oauth_client_secret: string;
    oauth_username?: string | null;
    oauth_password?: string | null;
    topic_name: string;
    webhook_url: string;
    pubsub_host?: string | null;
    tenant_id?: string | null;
    flow_batch_size: number;
    is_active: boolean;
    created_at?: string;
    updated_at?: string;
  }


  export type ListenerState = {
    client_id: number
    status: 'starting' | 'running' | 'stopping' | 'stopped' | 'error'
    started_at?: string | null
    last_beat?: string | null
    last_error?: string | null
    fail_count?: number
  }


export type Role = 'admin' | 'user'

export type User = {
  id: number
  email: string
  role: Role
  is_active: boolean
  created_at: string
}

export type UserCreatePayload = {
  email: string
  password: string
  role?: Role
  is_active?: boolean
}

export type UserUpdatePayload = {
  role?: Role
  is_active?: boolean
  password?: string // optional reset
}

  