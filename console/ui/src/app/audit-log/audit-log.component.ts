import {Component, OnInit, Injectable} from '@angular/core';
import {ActivatedRoute, ActivatedRouteSnapshot, Resolve, RouterStateSnapshot} from '@angular/router';
import {ConsoleService, AuditLogList, ApiAuditLog} from '../console.service';
import {Observable} from 'rxjs';
import {NgbDateStruct,NgbDate} from '@ng-bootstrap/ng-bootstrap';

const actions = new Map<number, string>([
    [1, 'Create'],
    [2, 'Update'],
    [3, 'Delete'],
    [4, 'Invoke'],
    [5, 'Import'],
    [6, 'Export'],
])

const resources = new Map<number, string>([
    [1, 'Account Wallet'],
    [2, 'Account Export'],
    [3, 'Account Friends'],
    [4, 'Account Groups'],
    [5, 'Account Notes'],
    [6, 'Acl Template'],
    [7, 'All Accounts'],
    [8, 'All Data'],
    [9, 'All Storage'],
    [10, 'Api Explorer'],
    [11, 'Audit Log'],
    [12, 'Configuration'],
    [13, 'Channel Message'],
    [14, 'User'],
    [15, 'Group'],
    [16, 'In App Purchase'],
    [17, 'Leaderboard'],
    [18, 'Leaderboard Record'],
    [19, 'Match'],
    [20, 'Notification'],
    [21, 'Satori Message'],
    [22, 'Settings'],
    [23, 'Storage Data'],
    [24, 'Storage Data Import'],
    [25, 'Hiro Inventory'],
    [26, 'Hiro Progression'],
    [27, 'Hiro Economy'],
    [28, 'Hiro Stats'],
    [29, 'Hiro Energy']
])

interface selectType {
    id: number,
    value: string
}

@Component({
    selector: 'app-audit-log',
    templateUrl: './audit-log.component.html',
    styleUrls: ['./audit-log.component.scss']
  })
export class AuditLogComponent implements OnInit {
    public auditLogList: Array<ApiAuditLog> = []
    public nextCursor = '';
    public prevCursor = '';
    public error = '';
    public limit = 20;
    public filter?: Record<string, string> = {};
    public usernames?: string[];
    public modelDatepicker:NgbDateStruct

    constructor(
        private readonly route: ActivatedRoute,
        private readonly consoleService: ConsoleService
    ){}

    ngOnInit():void {
        this.route.data.subscribe(
            d => {
              this.auditLogList.length = 0;
              if (d) {
                this.auditLogList.push(...d[0].entries);
                this.nextCursor = d[0].next_cursor;
                this.prevCursor = d[0].prev_cursor;
              }
            },
            err => {
              this.error = err;
        });

        this.consoleService.getUsernames('').subscribe(it => {
            this.usernames = it.usernames
        })
    }

    public loadData(cursor: string): void {
        this.error = '';
        this.consoleService.listAuditLog(
          '',
          this.limit,
          this.filter,
          cursor,
        ).subscribe(res => {
          this.auditLogList.length = 0;
          this.auditLogList.push(...res.entries);
          this.nextCursor = res.next_cursor;
          this.prevCursor = res.prev_cursor;
        }, error => {
          this.error = error;
        });
    }

    public onSelect(e:any, name:string): void {
        let value = ''
        if(name === 'after') {
           if (e instanceof NgbDate) {
            value = e.year + '-' + (e.month < 10 ? '0'+ e.month : e.month) + '-'+ (e.day < 10 ? '0'+ e.day : e.day)
           } else {
            value = (e.target as HTMLInputElement).value
           }

           value += 'T00:00:00.000Z'
        } else {
            value = (e.target as HTMLSelectElement).value
        }

        if(value === '') {
            delete(this.filter[name])
        } else {
           this.filter[name] = value
        }
        this.loadData('')
    }

    public toActionString(action: number):string {
        return actions.get(action) ?? 'Unkown'
    }

    public toResourceString(resource: number): string {
        return resources.get(resource) ?? 'Unkown'
    }

    public getResources():selectType[] {
        const items:selectType[] = []
        for (const [k, v] of resources) {
            items.push({id: k, value: v})
        }
        return items
    } 

    public getActions():selectType[] {
        const items:selectType[] = []
        for (const [k, v] of actions) {
            items.push({id: k, value: v})
        }
        return items
    } 
}

@Injectable({providedIn: 'root'})
export class AuditLogResolver implements Resolve<AuditLogList> {
  constructor(private readonly consoleService: ConsoleService) {}

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<AuditLogList> {
    return this.consoleService.listAuditLog('', 20);
  }
}