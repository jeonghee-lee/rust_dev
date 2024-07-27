use serde::{Serialize, Deserialize};
use memmap2::{MmapMut, MmapOptions};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::sync::RwLock;
use bincode;
use prettytable::{Table, Row, Cell, row};

use indexmap::IndexMap;
use std::sync::Arc;
use std::error::Error;
use log::{error, info};

use crate::parse_xml::FixError;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    pub id: u64,
    pub account: String,
    pub symbol: String,
    pub side: String,
    pub quantity: u64,
    pub price: u64,
    pub ordtype: String,
    pub transacttime: String,
    pub ordstatus: String,
}

pub struct OrderStore {
    orders: RwLock<HashMap<u64, Order>>,
    mmap: RwLock<MmapMut>,
}

impl OrderStore {
    pub fn new(file_path: &str, size: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        file.set_len(size as u64)?;

        let mmap = unsafe {
            MmapOptions::new().map_mut(&file)?
        };

        Ok(Self {
            orders: RwLock::new(HashMap::new()),
            mmap: RwLock::new(mmap),
        })
    }

    pub fn add_order(&self, order: Order) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut orders = self.orders.write().unwrap();
            orders.insert(order.id, order);
        } // Release the orders lock here before persisting
        self.persist()?;
        Ok(())
    }
    pub fn update_order(&self, order: Order) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut orders = self.orders.write().unwrap();
            if orders.contains_key(&order.id) {
                orders.insert(order.id, order);
            } else {
                return Err("Order ID not found".into());
            }
        }
        self.persist()?;
        Ok(())
    }

    pub fn get_order(&self, order_id: u64) -> Option<Order> {
        let orders = self.orders.read().unwrap();
        orders.get(&order_id).cloned()
    }

    pub fn remove_order(&self, order_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut orders = self.orders.write().unwrap();
            orders.remove(&order_id);
        } // Release the orders lock here before persisting
        self.persist()?;
        Ok(())
    }

    fn persist(&self) -> Result<(), Box<dyn std::error::Error>> {
        let serialized_orders;
        {
            let orders = self.orders.read().unwrap();
            serialized_orders = bincode::serialize(&*orders, bincode::Infinite)?;
        } // Release the orders lock after serialization

        if serialized_orders.len() > self.mmap.read().unwrap().len() {
            return Err("Serialized data exceeds mmap size".into());
        }

        let mut mmap = self.mmap.write().unwrap();
        mmap[..serialized_orders.len()].copy_from_slice(&serialized_orders);
        mmap.flush()?;
        Ok(())
    }

    pub fn load(&self) -> Result<(), Box<dyn std::error::Error>> {
        let orders;
        {
            let mmap = self.mmap.read().unwrap();
            if mmap.is_empty() {
                return Ok(());
            }
            orders = bincode::deserialize(&mmap[..mmap.len()])?;
        }

        {
            let mut orders_lock = self.orders.write().unwrap();
            *orders_lock = orders;
        }
        Ok(())
    }

    pub fn print_orders(&self) -> Result<String, FixError> {
        let orders = self.orders.read().unwrap();
        let mut table = Table::new();
        table.add_row(row!["ID", "Account", "Symbol", "Side", "Quantity", "Price", "OrdType", "TransactTime", "OrdStatus"]);

        for order in orders.values() {
            table.add_row(Row::new(vec![
                Cell::new(&order.id.to_string()),
                Cell::new(&order.account),
                Cell::new(&order.symbol),
                Cell::new(&order.side),
                Cell::new(&order.quantity.to_string()),
                Cell::new(&order.price.to_string()),
                Cell::new(&order.ordtype),
                Cell::new(&order.transacttime),
                Cell::new(&order.ordstatus),
            ]));
        }
        // table.printstd();
        // Convert the table to a string
        let table_string = format!("{}", table);
        Ok(table_string)
    }
}

pub fn add_order_to_store(order_store: Arc<OrderStore>, msg_map: &IndexMap<String, String>) -> Result<(), Box<dyn Error>> {
    let order = Order {
        id: msg_map.get("ClOrdID").unwrap().to_string().parse().expect("Invalid ClOrdID"),
        account: msg_map.get("Account").unwrap_or(&"".to_string()).to_string(),
        symbol: msg_map.get("Symbol").unwrap().to_string(),
        side: msg_map.get("Side").unwrap().to_string(),
        quantity: msg_map.get("OrderQty").unwrap().to_string().parse().expect("Invalid OrderQty"),
        price: msg_map.get("Price").unwrap().to_string().parse().expect("Invalid Price"),
        ordtype: msg_map.get("OrdType").unwrap().to_string(),
        transacttime: msg_map.get("TransactTime").unwrap().to_string(),
        ordstatus: msg_map.get("OrdStatus").unwrap().to_string(),
    };
    // order_store.add_order(order)?;
    match order_store.add_order(order.clone()) {
        Ok(_) => info!("Order added successfully: {:?}", order),
        Err(err) => error!("Failed to add order: {}", err),
    }
    Ok(())
}

pub fn update_order_in_store(order_store: Arc<OrderStore>, msg_map: &IndexMap<String, String>) -> Result<(), Box<dyn Error>> {
    let order = Order {
        id: msg_map.get("ClOrdID").unwrap().to_string().parse().expect("Invalid ClOrdID"),
        account: msg_map.get("Account").unwrap_or(&"".to_string()).to_string(),
        symbol: msg_map.get("Symbol").unwrap().to_string(),
        side: msg_map.get("Side").unwrap().to_string(),
        quantity: msg_map.get("OrderQty").unwrap().to_string().parse().expect("Invalid OrderQty"),
        price: msg_map.get("Price").unwrap().to_string().parse().expect("Invalid Price"),
        ordtype: msg_map.get("OrdType").unwrap().to_string(),
        transacttime: msg_map.get("TransactTime").unwrap().to_string(),
        ordstatus: msg_map.get("OrdStatus").unwrap().to_string(),
    };
    // order_store.update_order(order)?;
    match order_store.update_order(order.clone()) {
        Ok(_) => info!("Order updated successfully: {:?}", order),
        Err(err) => error!("Failed to update order: {}", err),
    }
    Ok(())
}

pub fn remove_order_from_store(order_store: Arc<OrderStore>, msg_map: &IndexMap<String, String>) -> Result<(), Box<dyn Error>> {
    let order_id = msg_map.get("ClOrdID").unwrap().to_string().parse().expect("Invalid ClOrdID");
    // order_store.remove_order(order_id)?;
    match order_store.remove_order(order_id) {
        Ok(_) => info!("Order removed successfully: {}", order_id),
        Err(err) => error!("Failed to remove order: {}", err),
    }
    Ok(())
}
