// Copyright 2020 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {Component, Injectable, OnInit, TemplateRef} from '@angular/core';
import {ActivatedRoute, ActivatedRouteSnapshot, Resolve, RouterStateSnapshot} from '@angular/router';
import {AddUserRequest, ConsoleService, PermissionTemplate, UserList, UserListUser, UserAcl, AddPermissionsTemplateRequest} from '../console.service';
import {UntypedFormBuilder, UntypedFormGroup, Validators} from '@angular/forms';
import {mergeMap, map} from 'rxjs/operators';
import {Observable} from 'rxjs';
import {DeleteConfirmService} from '../shared/delete-confirm.service';
import {NgbOffcanvas} from '@ng-bootstrap/ng-bootstrap';

const aclList: string[] = [
  "ACCOUNT",
  "ACCOUNT_EXPORT",
  "ACCOUNT_FRIENDS",
  "ACCOUNT_GROUPS",
  "ACCOUNT_NOTES",
  "ACCOUNT_WALLET",
  "ACL_TEMPLATE",
  "ALL_ACCOUNTS",
  "ALL_DATA",
  "ALL_STORAGE",
  "API_EXPLORER",
  "AUDIT_LOG",
  "CHANNEL_MESSAGE",
  "CONFIGURATION",
  "GROUP",
  "HIRO_ECONOMY",
  "HIRO_ENERGY",
  "HIRO_INVENTORY",
  "HIRO_PROGRESSION",
  "HIRO_STATS",
  "IN_APP_PURCHASE",
  "LEADERBOARD",
  "LEADERBOARD_RECORD",
  "MATCH",
  "NOTIFICATION",
  "SATORI_MESSAGE",
  "SETTINGS",
  "STORAGE_DATA",
  "STORAGE_DATA_IMPORT",
  "USER"
]

@Component({
  selector: 'app-users',
  templateUrl: './users.component.html',
  styleUrls: ['./users.component.scss']
})
export class UsersComponent implements OnInit {
  public error = '';
  public userCreateError = '';
  public successMessage = '';
  public users: Array<UserListUser> = [];
  public permissionsTemplates: Array<PermissionTemplate> = [];
  public createUserForm: UntypedFormGroup;
  public createPermissionsTemplateForm: UntypedFormGroup
  public createPermissionsTemplateError= ''
  public editUserPermissions?: UserListUser 

  constructor(
    private readonly route: ActivatedRoute,
    private readonly consoleService: ConsoleService,
    private readonly formBuilder: UntypedFormBuilder,
    private readonly deleteConfirmService: DeleteConfirmService,
    private readonly offcanvasService: NgbOffcanvas
  ) {}

  ngOnInit(): void {
    this.createUserForm = this.formBuilder.group({
      username: ['', Validators.required],
      email: ['', [Validators.required, Validators.email]],
      password: ['', Validators.compose([Validators.required, Validators.minLength(8)])],
      mfa: [true, Validators.required],
      newsletter: [false],
    });

    this.createPermissionsTemplateForm = this.formBuilder.group({
      id:[''],
      templateName:['', Validators.required],
      description:['']
    })

    this.route.data.subscribe(data => {
      const users = data[0] as UserList;
      this.users.length = 0;
      
      this.permissionsTemplates = []
      this.consoleService.getPermissionsTemplates('').subscribe((templateList) => {
        this.permissionsTemplates.push(...templateList.templates)
        users.users.forEach((user, index) => {
          users.users[index].aclName = this.getAclName(user.acl, templateList.templates)
        })
      })
     
      this.users.push(...users.users);
    }, err => {
      this.error = err;
    });
  }

  public getAclName(acls: Record<string, UserAcl>, templates:Array<PermissionTemplate>):string {
    let fullAccess = true;
    for (const acl of Object.values(acls)) {
      if (acl.read !== true || acl.delete !== true || acl.write !== true) {
        fullAccess = false;
        break;
      }
    }
    if (fullAccess) {
      return 'Full Access'
    }

    const matchedTemplate = templates.find(t => 
      JSON.stringify(t.acl) === JSON.stringify(acls)
    );

    if (matchedTemplate) {
      return matchedTemplate.name
    }
    return 'Custom'
  }

  public requireUserMfa(username: string, enforce: boolean): void {
    this.error = '';

    this.consoleService.requireUserMfa('', username, {required: enforce}).pipe(mergeMap(() => {
      return this.consoleService.listUsers('');
    })).subscribe((userList) => {
      this.error = '';
      this.users.length = 0;
      this.users.push(...userList.users);
      this.successMessage = `User ${username} Multi-factor authentication is now required`;
      setTimeout(() => {
        this.successMessage = '';
      }, 5000);
    }, error => {
      this.error = error;
    });
  }

  public resetUserMfa(username: string): void {
    this.error = '';

    this.consoleService.resetUserMfa('', username).pipe(mergeMap(() => {
      return this.consoleService.listUsers('');
    })).subscribe((userList) => {
      this.error = '';
      this.users.length = 0;
      this.users.push(...userList.users);
      this.successMessage = `User ${username} Multi-factor authentication was reset successfully`;
      setTimeout(() => {
        this.successMessage = '';
      }, 5000);
    }, error => {
      this.error = error;
    });
  }

  public deleteUser(username: string): void {
    this.error = '';

    this.deleteConfirmService.openDeleteConfirmModal(() => {
      this.consoleService.deleteUser('', username).pipe(mergeMap(() => {
        return this.consoleService.listUsers('');
      })).subscribe(userList => {
        this.error = '';
        this.users.length = 0;
        userList.users.forEach((user, index) => {
          userList.users[index].aclName = this.getAclName(user.acl, this.permissionsTemplates)
        })
        this.users.push(...userList.users);
      }, error => {
        this.error = error;
      });
    });
  }

  public addUser(): void {
    this.userCreateError = '';
    this.createUserForm.disable();

    const req: AddUserRequest = {
      username: this.f.username.value,
      email: this.f.email.value,
      password: this.f.password.value,
      newsletter_subscription: this.f.newsletter.value,
      mfa_required: this.f.mfa.value,
      acl: this.editUserPermissions.acl
    };

    this.consoleService.addUser('', req).pipe(mergeMap(() => {
        return this.consoleService.listUsers('');
    }))
    .subscribe(userList => {
      this.userCreateError = '';
      this.createUserForm.reset({mfa: true});
      this.createUserForm.enable();
      this.users.length = 0;

      userList.users.forEach((user, index) => {
        userList.users[index].aclName = this.getAclName(user.acl, this.permissionsTemplates)
      })
      this.users.push(...userList.users);
      this.closeOffcanvas(null)
  }, error => {
      this.userCreateError = error;
      this.createUserForm.enable();
    });
  }

  public addPermissionsTemplate():void {
    this.createPermissionsTemplateError = '';
    this.createPermissionsTemplateForm.disable();
    const req: AddPermissionsTemplateRequest = {
      name: this.fp.templateName.value,
      description: this.fp.description.value,
      acl: this.editUserPermissions.acl
    };

    const id = this.fp.id.value
    if (id === '') {
      this.consoleService.addPermissionsTemplate('', req)
      .subscribe(tm => {
        this.createPermissionsTemplateError = '';
        this.createPermissionsTemplateForm.enable();
        this.createPermissionsTemplateForm.reset()
        this.permissionsTemplates.push(tm);
        this.closeOffcanvas(null)
    }, error => {
        this.createPermissionsTemplateError = error;
        this.createPermissionsTemplateForm.enable();
      });
    } else {
      this.consoleService.updatePermissionsTemplate('', id, req)
      .subscribe(tm => {
        this.createPermissionsTemplateError = '';
        this.createPermissionsTemplateForm.enable();
        this.createPermissionsTemplateForm.reset()
        this.permissionsTemplates.forEach((item, key) => {
          if (item.id === id) {
            this.permissionsTemplates[key].name = tm.name
            this.permissionsTemplates[key].description = tm.description
            this.permissionsTemplates[key].acl = tm.acl
          }
        })
        this.closeOffcanvas(null)
    }, error => {
        this.createPermissionsTemplateError = error;
        this.createPermissionsTemplateForm.enable();
      });
    }
  }

  public updatePermissionsTemplate(content: TemplateRef<any>, template: PermissionTemplate): void {
    this.createPermissionsTemplateError = '';
    this.createPermissionsTemplateForm.reset();
    this.createPermissionsTemplateForm.setValue(
      {id:template.id, templateName: template.name, description: template.description}
    )

    this.editUserPermissions = template.acl
    this.openOffcanvas(content, {acl: template.acl})
  }

  public deletePermissionsTemplate(template: PermissionTemplate):void {
     this.consoleService.deletePermissionsTemplate('', template.id).subscribe({
      next: () => {
        this.permissionsTemplates = this.permissionsTemplates.filter((v) => v.id !== template.id)
      }
     })
  }

  public openOffcanvas(content: TemplateRef<any>, user:UserListUser):void {
    this.editUserPermissions = user
    this.offcanvasService.open(content, { 
      ariaLabelledBy: 'offcanvas-basic-title', 
      position: 'end', 
      backdrop: false,
      animation: true,
      panelClass: 'wide-offcanvas'
    }).result.then(
			(result) => {
        this.createUserForm.reset()
        this.createPermissionsTemplateForm.reset()
			},
			(reason) => {
        this.createUserForm.reset()
        this.createPermissionsTemplateForm.reset()
			},
		);
  }

  public closeOffcanvas(content: TemplateRef<any>):void {
    this.editUserPermissions = undefined
    this.createUserForm.reset()
    this.createPermissionsTemplateForm.reset()
    this.offcanvasService.dismiss()
  }

  public updatePermission(event: Event, key: string, value: string):void {
    if(!this.editUserPermissions.acl) return
    switch (value) {
      case 'read':
        this.editUserPermissions.acl[key].read = (event.target as HTMLInputElement).checked
        break
      case 'write':
        this.editUserPermissions.acl[key].write = (event.target as HTMLInputElement).checked
        break
      case 'delete':
        this.editUserPermissions.acl[key].delete = (event.target as HTMLInputElement).checked
        break
    }
  }

  public savePermission(): void {
    this.consoleService.updateUser('', this.editUserPermissions.username, { acl: this.editUserPermissions.acl })
      .pipe(
        mergeMap(() => this.consoleService.listUsers('')),
        mergeMap(userList => {
          // 重新计算 aclName（复用 ngOnInit 中的逻辑）
          return this.consoleService.getPermissionsTemplates('').pipe(
            map(templateList => ({ userList, templates: templateList.templates }))
          );
        })
      )
      .subscribe({
        next: ({ userList, templates }) => {
          this.permissionsTemplates = templates;
          this.users = userList.users.map(user => {
            return { ...user, aclName: this.getAclName(user.acl, templates) };
          });
          this.closeOffcanvas(null);
        },
        error: (err) => {
          this.error = err;
        }
      });
  }

  public formatKey(key: string): string {
    return key
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' ');
  }

  public updatePermissionByTemplate(template:PermissionTemplate | 'CLEAR' | 'ALL'):void {
    switch (template) {
      case 'CLEAR':
        if(!this.editUserPermissions.acl) {
          return
        }

        for (const acl of Object.keys(this.editUserPermissions.acl)) {
          this.editUserPermissions.acl[acl].read = false
          this.editUserPermissions.acl[acl].write = false
          this.editUserPermissions.acl[acl].delete = false
        }
        break
      case 'ALL':
        if(!this.editUserPermissions.acl) {
          return
        }

        for (const acl of Object.keys(this.editUserPermissions.acl)) {
          this.editUserPermissions.acl[acl].read = true
          this.editUserPermissions.acl[acl].write = true
          this.editUserPermissions.acl[acl].delete = true
        }
        break
      default:
        for (const acl of Object.keys(template.acl)) {
          this.editUserPermissions.acl[acl].read = template.acl[acl].read
          this.editUserPermissions.acl[acl].write = template.acl[acl].write
          this.editUserPermissions.acl[acl].delete = template.acl[acl].delete
        }
        break
    }
  }

  public preTemplate():PermissionTemplate {
    const template:PermissionTemplate = {
      acl:{}
    }

    aclList.forEach( v => template.acl[v] = {read: false, write: false, delete: false})
    return template
  }

  get f(): any {
    return this.createUserForm.controls;
  }

  get fp(): any{
    return this.createPermissionsTemplateForm.controls;
  }
}

@Injectable({providedIn: 'root'})
export class UsersResolver implements Resolve<UserList> {
  constructor(private readonly consoleService: ConsoleService) {}

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<UserList> {
    return this.consoleService.listUsers('');
  }
}
